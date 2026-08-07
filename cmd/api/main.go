// Command api runs the ATOS Phase 0 gateway: REST + MCP surfaces backed by
// an in-memory store and mock tos-ai/tos-core adapters, per
// ~/atos-spec/docs/IMPLEMENTATION_ROADMAP.md's "Phase 0 — Contract First".
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

	tosaimock "github.com/tosnetwork/atos/internal/adapters/tosai/mock"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/config"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/httpapi"
	"github.com/tosnetwork/atos/internal/mcp"
	"github.com/tosnetwork/atos/internal/service"
	"github.com/tosnetwork/atos/internal/store/memory"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	st := memory.New()
	tosai := tosaimock.New()
	toscore := toscoremock.New(st)

	capabilities := service.NewCapabilityService(st)
	quotes := service.NewQuoteService(st)
	accounts := service.NewAccountService(st)
	jobs := service.NewJobService(st, tosai, toscore, accounts)

	if err := seedDemoCapability(capabilities); err != nil {
		logger.Error("failed to seed demo capability", "error", err)
		os.Exit(1)
	}

	restServer := &httpapi.Server{
		Capabilities: capabilities,
		Quotes:       quotes,
		Jobs:         jobs,
		Accounts:     accounts,
		Logger:       logger,
	}
	mcpServer := &mcp.Server{
		Capabilities: capabilities,
		Quotes:       quotes,
		Jobs:         jobs,
		Accounts:     accounts,
		Logger:       logger,
	}

	mux := restServer.Mux()
	mux.HandleFunc("POST /mcp", mcpServer.Handler())

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           logRequests(logger, mux),
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

func logRequests(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
	})
}

// seedDemoCapability registers one sandbox capability at startup so
// atos_search/atos_quote/atos_invoke have something real to exercise
// immediately after `go run ./cmd/api` — see the README quickstart.
func seedDemoCapability(capabilities *service.CapabilityService) error {
	_, err := capabilities.Register(context.Background(), service.RegisterCapabilityInput{
		ProviderID:   "agt_demo",
		Name:         "Echo Sandbox",
		Description:  "Returns its input unchanged. For exercising the ATOS contract end to end, not real work.",
		DeliveryMode: domain.DeliveryInstant,
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
		Pricing: domain.Pricing{
			Model:     domain.PricingFixed,
			PriceHint: domain.PriceHint{Amount: "1.00", Currency: "USD"},
		},
		Tags: []string{"sandbox", "demo"},
	})
	return err
}
