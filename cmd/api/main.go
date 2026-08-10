// Command api runs the ATOS REST, MCP and A2A gateway.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	if cfg.DatabaseURL != "" {
		pg, err := postgres.Open(context.Background(), cfg.DatabaseURL)
		if err != nil {
			logger.Error("postgres connect failed", "error", err)
			os.Exit(1)
		}
		defer pg.Close()
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
		logger.Info("using tos-protocol RPC backend", "url", cfg.TOSRPC.URL)
	default:
		logger.Error("unsupported TOS backend", "backend", cfg.TOSBackend)
		os.Exit(2)
	}

	// Wraps whichever native execution backend was just selected: a
	// Capability whose binding is tos-native/human/absent executes exactly
	// as before (unchanged behavior); one bound to http/mcp/a2a routes
	// through the matching outbound provider adapter instead. JobService
	// and StreamService still talk to exactly one tosai.Provider -- this
	// is the only place execution fans out by transport, never a second
	// execution path. See internal/adapters/tosai/dispatch's package doc.
	execution = dispatch.New(execution, provideradapter.NewResolver(
		httpadapter.New(httpadapter.Config{}),
		mcpadapter.New(mcpadapter.Config{}),
		a2aadapter.New(a2aadapter.Config{}),
	))

	capabilities := service.NewCapabilityService(st)
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
	quotes.WithAccountService(accounts)
	jobs := service.NewJobService(st, execution, core, accounts)
	streams := service.NewStreamService(st, execution)
	receipts := service.NewReceiptService(st, core)

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
	jobs.WithEarnings(earnings)

	blobStorage, err := local.New(cfg.BlobDir, cfg.PublicBaseURL, st)
	if err != nil {
		logger.Error("storage init failed", "error", err)
		os.Exit(1)
	}
	artifacts := service.NewArtifactService(st, blobStorage)
	disputes := service.NewDisputeService(st, jobs, earnings, accounts, artifacts)

	reconcileCtx, reconcileCancel := context.WithCancel(context.Background())
	defer reconcileCancel()
	go jobs.RunReconciler(reconcileCtx, 15*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("managed economic reconciliation pending", "error", reconcileErr)
	})
	go earnings.RunReconciler(reconcileCtx, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("provider earnings reconciliation pending", "error", reconcileErr)
	})
	go disputes.RunReconciler(reconcileCtx, 20*time.Second, 30*time.Second, 100, func(reconcileErr error) {
		logger.Error("dispute economic reconciliation pending", "error", reconcileErr)
	})

	if err := seedDemoCapability(capabilities, cfg.TOSBackend); err != nil {
		logger.Error("failed to seed demo capability", "error", err)
		os.Exit(1)
	}

	restServer := &httpapi.Server{
		Auth: authorization, Capabilities: capabilities, Quotes: quotes,
		Jobs: jobs, Streams: streams, Accounts: accounts, Receipts: receipts,
		Earnings: earnings, Disputes: disputes, Artifacts: artifacts, Logger: logger, PublicBaseURL: cfg.PublicBaseURL,
		ApprovalToken: cfg.Auth.ApprovalToken,
	}
	mcpServer := &mcp.Server{
		Auth: authorization, Capabilities: capabilities, Quotes: quotes,
		Jobs: jobs, Accounts: accounts, Receipts: receipts, Earnings: earnings,
		Disputes: disputes, Artifacts: artifacts, Logger: logger, PublicBaseURL: cfg.PublicBaseURL,
	}
	a2aServer := &a2a.Server{
		Auth: authorization, Quotes: quotes, Jobs: jobs,
		Logger: logger, PublicBaseURL: cfg.PublicBaseURL,
	}

	mux := restServer.Mux()
	mux.HandleFunc("POST /mcp", mcpServer.Handler())
	mux.HandleFunc("POST /a2a", a2aServer.Handler())
	mux.HandleFunc("/v1/blob/", blobStorage.BlobHandler())

	httpServer := &http.Server{
		Addr: cfg.Addr, Handler: observability.Middleware(logger, mux),
		ReadHeaderTimeout: 5 * time.Second,
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
