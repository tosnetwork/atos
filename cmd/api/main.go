// Command api runs the ATOS REST, MCP and A2A gateway.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/jackc/pgx/v5"

	"github.com/tosnetwork/atos/internal/a2a"
	"github.com/tosnetwork/atos/internal/adapters/payout"
	payoutmock "github.com/tosnetwork/atos/internal/adapters/payout/mock"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/a2aadapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/httpadapter"
	"github.com/tosnetwork/atos/internal/adapters/provideradapter/mcpadapter"
	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/tosai/dispatch"
	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/auth"
	"github.com/tosnetwork/atos/internal/config"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/financial"
	"github.com/tosnetwork/atos/internal/httpapi"
	"github.com/tosnetwork/atos/internal/mcp"
	"github.com/tosnetwork/atos/internal/observability"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/atos/internal/store/memory"
	"github.com/tosnetwork/atos/internal/store/postgres"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(2)
	}

	var st store.Store
	var pgStore *postgres.Store
	if cfg.DatabaseURL != "" {
		pg, err := postgres.Open(context.Background(), cfg.DatabaseURL)
		if err != nil {
			logger.Error("postgres connect failed", "error", err)
			os.Exit(1)
		}
		defer pg.Close()
		pgStore = pg
		st = pg
		logger.Info("using postgres store")
	} else {
		st = memory.New()
		logger.Info("using in-memory store (set ATOS_DATABASE_URL for postgres)")
	}

	var authPersistence auth.Persistence
	if cfg.Auth.StatePath != "" {
		authPersistence, err = auth.OpenBoltPersistence(cfg.Auth.StatePath)
		if err != nil {
			logger.Error("authorization state init failed", "error", err)
			os.Exit(1)
		}
	}
	authorization, err := auth.Open(auth.Config{
		AutoApprove:  cfg.Auth.AutoApprove,
		TokenTTL:     cfg.Auth.TokenTTL,
		DeviceTTL:    cfg.Auth.DeviceTTL,
		PollInterval: cfg.Auth.PollInterval,
		Persistence:  authPersistence,
	})
	if err != nil {
		logger.Error("authorization service init failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = authorization.Close() }()
	var execution tosai.Provider
	var core toscore.Core
	var quoter tosai.Quoter
	var anchorPublisher financial.AnchorPublisher
	// remoteProber, when set, routes HealthService's readiness probing
	// through the same execution/data-plane boundary as third-party
	// execution (see service.ThirdPartyHealthProber's doc comment) instead
	// of dialing binding.EndpointRef locally.
	var remoteProber service.ThirdPartyHealthProber
	switch cfg.TOSBackend {
	case config.TOSBackendMock:
		execution = tosaimock.New()
		core = toscoremock.New(st)
		logger.Info("using explicit mock TOS backend")
	case config.TOSBackendRPC:
		rpcClient, rpcErr := toprotocol.New(toprotocol.Config{
			BaseURL: cfg.TOSRPC.URL, BearerToken: cfg.TOSRPC.Token,
			Timeout: cfg.TOSRPC.Timeout, MaxMessageBytes: cfg.TOSRPC.MaxMessageBytes,
			Insecure: cfg.TOSRPC.Insecure, ServerName: cfg.TOSRPC.ServerName,
			CAFile: cfg.TOSRPC.CAFile, ClientCertFile: cfg.TOSRPC.ClientCertFile,
			ClientKeyFile: cfg.TOSRPC.ClientKeyFile, Store: st,
			Network: cfg.TOSRPC.Network,
		})
		if rpcErr != nil {
			logger.Error("configure tos-protocol RPC backend", "error", rpcErr)
			os.Exit(2)
		}
		readyCtx, cancel := context.WithTimeout(context.Background(), cfg.TOSRPC.Timeout)
		rpcErr = rpcClient.CheckReady(readyCtx)
		cancel()
		if rpcErr != nil {
			logger.Error("tos-protocol RPC backend is unavailable", "error", rpcErr)
			os.Exit(1)
		}
		defer rpcClient.Close()
		execution, core, quoter = rpcClient, rpcClient, rpcClient
		anchorPublisher = rpcClient
		if cfg.RemoteThirdPartyExecution {
			remoteProber = rpcClient
		}
		logger.Info("using tos-protocol RPC backend", "url", cfg.TOSRPC.URL)
	default:
		logger.Error("unsupported TOS backend", "backend", cfg.TOSBackend)
		os.Exit(2)
	}

	// Shared by dispatch's third-party execution routing below and by
	// HealthService's local (non-remote-probed) readiness checks --
	// dialing binding.EndpointRef is the same outbound concern in both
	// cases, just for two different purposes (executing vs. probing).
	providerResolver := provideradapter.NewResolver(
		httpadapter.New(httpadapter.Config{}),
		mcpadapter.New(mcpadapter.Config{}),
		a2aadapter.New(a2aadapter.Config{}),
	)

	// Wraps whichever native execution backend was just selected: a
	// Capability whose binding is tos-native/human/absent executes exactly
	// as before (unchanged behavior); one bound to http/mcp/a2a routes
	// through the matching outbound provider adapter instead. JobService
	// and StreamService still talk to exactly one tosai.Provider -- this
	// is the only place execution fans out by transport, never a second
	// execution path. See internal/adapters/tosai/dispatch's package doc.
	execution = dispatch.New(execution, providerResolver,
		dispatch.WithRemoteThirdPartyExecution(cfg.RemoteThirdPartyExecution))

	capabilities := service.NewCapabilityService(st)
	health := service.NewHealthService(st, capabilities, providerResolver)
	if remoteProber != nil {
		health.WithRemoteProber(remoteProber)
	}
	var quotes *service.QuoteService
	if quoter == nil {
		quotes = service.NewQuoteService(st)
	} else {
		quotes = service.NewQuoteService(st, quoter)
	}
	accountDefaults := service.AccountDefaults{
		InitialBalance: domain.Money{Amount: cfg.ManagedAccount.InitialBalance, Currency: cfg.ManagedAccount.Currency},
		PerCallLimit:   domain.Money{Amount: cfg.ManagedAccount.PerCallLimit, Currency: cfg.ManagedAccount.Currency},
		DailyLimit:     domain.Money{Amount: cfg.ManagedAccount.DailyLimit, Currency: cfg.ManagedAccount.Currency},
	}
	if err := service.ValidateAccountDefaults(accountDefaults); err != nil {
		logger.Error("invalid managed account configuration", "error", err)
		os.Exit(2)
	}
	accounts := service.NewAccountService(st, accountDefaults)
	var financialAdapter *financial.Adapter
	var financialRepository *financial.Repository
	var financialBlnk *financial.BlnkClient
	if cfg.Financial.Backend == config.FinancialBackendBlnk {
		if pgStore == nil {
			logger.Error("Blnk financial backend requires PostgreSQL")
			os.Exit(2)
		}
		repository, repoErr := financial.NewRepository(pgStore.Pool(), cfg.Financial.GatewayID, cfg.Financial.NetworkID)
		if repoErr != nil {
			logger.Error("financial repository init failed", "error", repoErr)
			os.Exit(2)
		}
		blnkClient, blnkErr := financial.NewBlnkClient(financial.BlnkConfig{
			BaseURL: cfg.Financial.BlnkURL, APIKey: cfg.Financial.BlnkKey, Timeout: cfg.Financial.Timeout,
			GenesisIssuanceLimit: cfg.Financial.IssuanceLimit,
		})
		if blnkErr != nil {
			logger.Error("Blnk financial client init failed", "error", blnkErr)
			os.Exit(2)
		}
		financialAdapter, err = financial.NewAdapter(repository, blnkClient)
		if err != nil {
			logger.Error("financial adapter init failed", "error", err)
			os.Exit(2)
		}
		financialRepository = repository
		financialBlnk = blnkClient
		accounts.WithFinancialAuthority(financialAdapter)
		logger.Info("using Blnk as authoritative managed financial ledger", "gateway_id", cfg.Financial.GatewayID, "network_id", cfg.Financial.NetworkID)
	}
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, execution, core, accounts)
	if financialAdapter != nil {
		jobs.WithFinancialAuthority(financialAdapter)
	}
	streams := service.NewStreamService(st, execution)
	receipts := service.NewReceiptService(st, core)
	proofPackages := service.NewPortableProofService(st, core)

	// No production payout rail exists yet. ATOS_PAYOUT_BACKEND defaults to
	// "disabled": earnings still mature to Available and stop there --
	// nothing ever attempts an external payout, so the ledger can never
	// mark a real provider liability "paid" when no funds actually moved.
	// "mock" is an explicit, deterministic test/development opt-in (see
	// internal/adapters/payout/mock) that never moves real funds either,
	// but DOES drive earnings through to Paid, purely to exercise the
	// ledger and payout state machine end to end; config.Validate rejects
	// it in production. Swap in a real payout.Adapter implementation and a
	// new backend case here before any deployment pays anyone for real.
	var payoutAdapter payout.Adapter
	switch cfg.PayoutBackend {
	case config.PayoutBackendMock:
		payoutAdapter = payoutmock.New()
		logger.Warn("ATOS_PAYOUT_BACKEND=mock: using the mock payout adapter, which never moves real funds (development/test only)")
	default:
		logger.Info("ATOS_PAYOUT_BACKEND=disabled: provider earnings will mature to available and stop there; no payout will be attempted")
	}
	earnings := service.NewEarningsService(st, payoutAdapter)
	if financialAdapter != nil {
		earnings.WithFinancialAuthority(financialAdapter)
	}
	jobs.WithEarnings(earnings)

	blobStorage, err := local.New(cfg.BlobDir, cfg.PublicBaseURL, st)
	if err != nil {
		logger.Error("storage init failed", "error", err)
		os.Exit(1)
	}
	artifacts := service.NewArtifactService(st, blobStorage)
	disputes := service.NewDisputeService(st, jobs, earnings, accounts, artifacts)
	if financialAdapter != nil {
		disputes.WithFinancialAuthority(financialAdapter)
	}
	executionSigners := service.NewExecutionSignerService(st, core, capabilities)
	if cfg.TOSBackend == config.TOSBackendRPC && cfg.TOSRPC.Network != "" {
		quotes.WithVerifiedCommitmentAuthority(core, executionSigners, "atos.im")
	}
	certifications := service.NewCertificationService(st, capabilities, providerResolver)
	if remoteProber != nil {
		certifications.WithRemoteProber(remoteProber)
	}
	// Phase 4A: capability registration anchors its manifest/ownership
	// commitment through core (real tos-protocol RPC, or the mock's
	// in-process simulation under ATOS_TOS_BACKEND=mock) whenever a
	// capability requests any non-Managed mode -- see
	// CapabilityService.WithManifestAnchor's doc comment.
	capabilities.WithManifestAnchor(core)
	identityBindings := service.NewIdentityBindingService(st, core)

	// domain.ActivationAuthority: TOSBackedActivationAuthority is only
	// wired when this deployment is both on the real RPC backend AND has
	// an explicit ATOS_TOS_NETWORK configured -- either condition missing
	// keeps FailClosedActivationAuthority, exactly as Phase 3B's own
	// "production has no implementation that ever returns granted=true
	// until Phase 4 supplies a real authority" default. The mock backend
	// deliberately never gets the real authority: it exists for local
	// dev/test, and its CommitCapabilityManifest/identity-binding
	// simulation must never be presented as a real TOS guarantee (atos-spec
	// docs/IMPLEMENTATION_ROADMAP.md §8.1).
	var activationAuthority domain.ActivationAuthority = service.FailClosedActivationAuthority{}
	tosBackedAuthorityWired := cfg.TOSBackend == config.TOSBackendRPC && cfg.TOSRPC.Network != ""
	if tosBackedAuthorityWired {
		activationAuthority = service.NewTOSBackedActivationAuthority(core, st, executionSigners)
		logger.Info("using TOS-backed ActivationAuthority", "network", cfg.TOSRPC.Network)
	} else {
		logger.Info("using fail-closed ActivationAuthority (verified/native activation is unavailable)")
	}
	openTasks := service.NewOpenTaskService(st, quotes, jobs)

	// Passkey/WebAuthn human account authentication (atos-spec
	// docs/AUTH.md's "Human Account Authentication (Passkey/WebAuthn)"
	// section) is opt-in: a deployment that hasn't set ATOS_WEBAUTHN_RP_ID
	// gets a PasskeyService with a nil webAuthn instance, which every
	// method already fails closed against (ErrPasskeyNotConfigured) rather
	// than this process refusing to start.
	var webAuthnInstance *webauthn.WebAuthn
	if cfg.WebAuthn.RPID != "" {
		webAuthnInstance, err = webauthn.New(&webauthn.Config{
			RPID: cfg.WebAuthn.RPID, RPDisplayName: cfg.WebAuthn.RPDisplayName, RPOrigins: cfg.WebAuthn.RPOrigins,
		})
		if err != nil {
			logger.Error("initialize WebAuthn failed", "error", err)
			os.Exit(1)
		}
	}
	passkeys := service.NewPasskeyService(st, webAuthnInstance, authorization)

	// config.Config.Validate already confirmed every entry parses as a
	// CIDR, so this only ever fails on a config/validate drift, not on
	// anything an operator can trigger by misconfiguring
	// ATOS_TRUSTED_PROXY_CIDRS at runtime.
	trustedProxyCIDRs := make([]netip.Prefix, 0, len(cfg.TrustedProxyCIDRs))
	for _, raw := range cfg.TrustedProxyCIDRs {
		parsed, err := netip.ParsePrefix(raw)
		if err != nil {
			logger.Error("invalid trusted proxy CIDR", "cidr", raw, "error", err)
			os.Exit(2)
		}
		trustedProxyCIDRs = append(trustedProxyCIDRs, parsed)
	}

	reconcileCtx, reconcileCancel := context.WithCancel(context.Background())
	defer reconcileCancel()
	if financialRepository != nil && cfg.Financial.SignerURL != "" && cfg.Financial.SignerToken != "" && cfg.Financial.SigningKeyID != "" && cfg.Financial.SigningPublicKey != "" && cfg.Financial.RetentionURL != "" && cfg.Financial.RetentionHMACKey != "" && anchorPublisher != nil {
		signer, signerErr := financial.NewHTTPSigner(cfg.Financial.SignerURL, cfg.Financial.SigningKeyID, cfg.Financial.SigningAlgorithm, cfg.Financial.SignerToken, cfg.Financial.Timeout)
		if signerErr != nil {
			logger.Error("financial signer init failed", "error", signerErr)
			os.Exit(2)
		}
		retainer, retainerErr := financial.NewHTTPRetainer(cfg.Financial.RetentionURL, cfg.Financial.RetentionHMACKey, cfg.Financial.Timeout, cfg.Financial.MinimumRetention)
		if retainerErr != nil {
			logger.Error("financial WORM retainer init failed", "error", retainerErr)
			os.Exit(2)
		}
		go func() {
			ticker := time.NewTicker(cfg.Financial.SealInterval)
			defer ticker.Stop()
			for {
				select {
				case <-reconcileCtx.Done():
					return
				case <-ticker.C:
					batch, sealErr := financialRepository.SealNext(reconcileCtx, financialBlnk, signer, cfg.Financial.SigningKeyID, cfg.Financial.SigningAlgorithm, cfg.Financial.SigningPublicKey, retainer, anchorPublisher, cfg.Financial.BatchSize)
					if sealErr != nil && !errors.Is(sealErr, pgx.ErrNoRows) {
						logger.Error("financial batch sealing failed", "error", sealErr)
						if entered, healthErr := financialRepository.EnforceSealingHealth(reconcileCtx, cfg.Financial.MaxAnchorLag, sealErr); healthErr != nil {
							logger.Error("financial sealing health enforcement failed", "error", healthErr)
						} else if entered {
							logger.Error("financial safe mode entered after sealing integrity/lag failure")
						}
					}
					if sealErr == nil {
						logger.Info("financial batch externally sealed and anchored", "batch_id", batch.Manifest.BatchID, "merkle_root", batch.Manifest.MerkleRoot)
					}
				}
			}
		}()
	}
	if financialAdapter != nil {
		go func() {
			recoveryTicker := time.NewTicker(10 * time.Second)
			fullAuditTicker := time.NewTicker(cfg.Financial.FullAuditInterval)
			defer recoveryTicker.Stop()
			defer fullAuditTicker.Stop()
			run := func(full bool) {
				var result financial.ReconcileResult
				var reconcileErr error
				if full {
					result, reconcileErr = financialAdapter.Reconcile(reconcileCtx, 100)
				} else {
					result, reconcileErr = financialAdapter.RecoverPending(reconcileCtx, 100)
				}
				if reconcileErr != nil {
					logger.Error("financial reconciliation failed", "full_audit", full, "error", reconcileErr)
				} else if result.Mismatches > 0 || result.SafeMode {
					logger.Error("financial integrity safe mode", "full_audit", full, "mismatches", result.Mismatches, "safe_mode", result.SafeMode)
				}
			}
			run(true)
			for {
				select {
				case <-reconcileCtx.Done():
					return
				case <-recoveryTicker.C:
					run(false)
				case <-fullAuditTicker.C:
					run(true)
				}
			}
		}()
	}
	go jobs.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("managed economic reconciliation pending", "error", reconcileErr)
	})
	go earnings.RunReconciler(reconcileCtx, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("provider earnings reconciliation pending", "error", reconcileErr)
	})
	go disputes.RunReconciler(reconcileCtx, 20*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("dispute economic reconciliation pending", "error", reconcileErr)
	})
	go health.RunReconciler(reconcileCtx, 5*time.Minute, 200, func(reconcileErr error) {
		logger.Error("provider health sweep pending", "error", reconcileErr)
	})
	go executionSigners.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("execution-signer operation reconciliation pending", "error", reconcileErr)
	})
	go quotes.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("verified quote commitment reconciliation pending", "error", reconcileErr)
	})
	go openTasks.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("open task acceptance reconciliation pending", "error", reconcileErr)
	})
	go identityBindings.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("identity-binding operation reconciliation pending", "error", reconcileErr)
	})
	go proofPackages.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) { logger.Error("portable proof reconciliation failed", "error", reconcileErr) })
	// Only run the suspension sweep when a REAL authority is wired.
	// FailClosedActivationAuthority always returns granted=false with a nil
	// error -- indistinguishable, from SweepVerified's perspective, from a
	// real authority genuinely finding every active capability invalid.
	// Running this sweep against the fail-closed placeholder would mass-
	// suspend every already-active Verified capability on its very first
	// (immediate, startup) sweep the moment this deployment lacks a real
	// authority (e.g. ATOS_TOS_NETWORK accidentally unset, or a maintenance
	// restart on the mock backend) -- turning a pure configuration gap into
	// live suspensions of previously-legitimate capabilities, rather than
	// just correctly blocking NEW activations the way fail-closed should.
	if tosBackedAuthorityWired {
		identityEvidence := service.NewIdentityEvidenceReconciler(capabilities, activationAuthority)
		go identityEvidence.RunReconciler(reconcileCtx, 5*time.Minute, 200, func(reconcileErr error) {
			logger.Error("identity-evidence suspension sweep pending", "error", reconcileErr)
		})
	}
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-reconcileCtx.Done():
				return
			case <-ticker.C:
				if _, err := passkeys.PurgeExpiredCeremonies(reconcileCtx); err != nil {
					logger.Error("passkey ceremony purge failed", "error", err)
				}
				passkeys.PurgeStaleRateLimitEntries()
			}
		}
	}()

	if err := seedDemoCapability(capabilities, cfg.TOSBackend); err != nil {
		logger.Error("failed to seed demo capability", "error", err)
		os.Exit(1)
	}

	restServer := &httpapi.Server{
		Auth: authorization, Capabilities: capabilities, Health: health, ExecutionSigners: executionSigners,
		Certifications:      certifications,
		Passkeys:            passkeys,
		ActivationAuthority: activationAuthority, IdentityBindings: identityBindings, OpenTasks: openTasks, Quotes: quotes,
		Jobs: jobs, Streams: streams, Accounts: accounts, Receipts: receipts, ProofPackages: proofPackages,
		Earnings: earnings, Disputes: disputes, Artifacts: artifacts, Logger: logger, PublicBaseURL: cfg.PublicBaseURL,
		ApprovalToken: cfg.Auth.ApprovalToken, AdminApprovalToken: cfg.Auth.AdminApprovalToken,
		TrustedProxyCIDRs: trustedProxyCIDRs,
	}
	mcpServer := &mcp.Server{
		Auth: authorization, Capabilities: capabilities, Health: health, ExecutionSigners: executionSigners,
		Certifications:      certifications,
		ActivationAuthority: activationAuthority, IdentityBindings: identityBindings, OpenTasks: openTasks, Quotes: quotes,
		Jobs: jobs, Accounts: accounts, Receipts: receipts, ProofPackages: proofPackages, Earnings: earnings,
		Disputes: disputes, Artifacts: artifacts, Logger: logger, PublicBaseURL: cfg.PublicBaseURL,
	}
	a2aServer := &a2a.Server{
		Auth: authorization, Quotes: quotes, Jobs: jobs, OpenTasks: openTasks, ProofPackages: proofPackages,
		Logger: logger, PublicBaseURL: cfg.PublicBaseURL,
	}

	mux := restServer.Mux()
	mux.HandleFunc("POST /mcp", mcpServer.Handler())
	mux.HandleFunc("POST /a2a", a2aServer.Handler())
	mux.HandleFunc("/v1/blob/", blobStorage.BlobHandler())
	if financialRepository != nil {
		mux.Handle("GET /internal/financial-integrity/metrics", financial.MetricsHandler(financialRepository, 5*time.Second))
	}

	httpServer := &http.Server{
		Addr: cfg.Addr, Handler: observability.Middleware(logger, mux),
		ReadHeaderTimeout: 5 * time.Second,
		// IdleTimeout bounds how long a keep-alive connection may sit idle
		// between requests -- safe to set unconditionally, since it only
		// applies between requests, never during one. A blanket
		// ReadTimeout/WriteTimeout is deliberately NOT set here: this
		// server also handles GET /v1/jobs/{id}/stream (a genuine SSE
		// endpoint with its own 5-minute internal bound, internal/httpapi/
		// stream.go's streamMaxWait) and blob uploads with a 15-minute
		// upload TTL (internal/adapters/storage/local's uploadTTL) --
		// either timeout would need a careful per-route audit to avoid
		// silently truncating those, which is out of scope for the
		// specific anonymous-body-size fix this hardens (see
		// internal/httpapi/passkey.go's http.MaxBytesReader on the two
		// truly anonymous passkey finish routes instead).
		IdleTimeout: 120 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		logger.Info("atos listening", "addr", cfg.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}

func seedDemoCapability(capabilities *service.CapabilityService, backend config.TOSBackend) error {
	endpointRef := "internal:mock"
	if backend == config.TOSBackendRPC {
		endpointRef = "tos-protocol:execution-gateway"
	}
	_, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID: "agt_demo", Name: "Echo Sandbox",
		Description:  "Returns its input unchanged. For exercising the ATOS v0.2 Managed contract end to end.",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		Pricing:             domain.Pricing{Model: domain.PricingFixed, PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"}},
		Tags:                []string{"sandbox", "demo"},
		RequestedTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		Bindings: []domain.CapabilityBinding{{
			Transport: domain.AdapterTOSNative, EndpointRef: endpointRef,
			EligibleTrustModes: []domain.TrustMode{domain.TrustModeManaged},
		}},
		IdempotencyKey: "seed-echo-sandbox-v1",
	})
	return err
}
