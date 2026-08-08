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
	"github.com/tosnetwork/atos/internal/adapters/storage/local"
	"github.com/tosnetwork/atos/internal/adapters/tosai"
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

	authorization := auth.NewService()
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

	capabilities := service.NewCapabilityService(st)
	var quotes *service.QuoteService
	if quoter == nil {
		quotes = service.NewQuoteService(st)
	} else {
		quotes = service.NewQuoteService(st, quoter)
	}
	accounts := service.NewAccountService(st)
	jobs := service.NewJobService(st, execution, core, accounts)
	receipts := service.NewReceiptService(st, core)

	blobStorage, err := local.New(cfg.BlobDir, cfg.PublicBaseURL, st)
	if err != nil {
		logger.Error("storage init failed", "error", err)
		os.Exit(1)
	}
	artifacts := service.NewArtifactService(st, blobStorage)
	if err := seedDemoCapability(capabilities, cfg.TOSBackend); err != nil {
		logger.Error("failed to seed demo capability", "error", err)
		os.Exit(1)
	}

	restServer := &httpapi.Server{
		Auth: authorization, Capabilities: capabilities, Quotes: quotes,
		Jobs: jobs, Accounts: accounts, Receipts: receipts,
		Artifacts: artifacts, Logger: logger,
	}
	mcpServer := &mcp.Server{
		Auth: authorization, Capabilities: capabilities, Quotes: quotes,
		Jobs: jobs, Accounts: accounts, Receipts: receipts,
		Artifacts: artifacts, Logger: logger,
	}
	a2aServer := &a2a.Server{Auth: authorization, Quotes: quotes, Jobs: jobs, Logger: logger}

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
