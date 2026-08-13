// Command api runs the stateless atos_native_v1 gateway.
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

	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/config"
	"github.com/tosnetwork/atos/internal/nativegateway"
	"github.com/tosnetwork/tos-protocol/gen/atos/native/v1/atosnativev1connect"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(2)
	}

	backend, err := toprotocol.New(toprotocol.Config{
		BaseURL: cfg.TOSRPC.URL, BearerToken: cfg.TOSRPC.Token,
		Timeout: cfg.TOSRPC.Timeout, MaxMessageBytes: cfg.TOSRPC.MaxMessageBytes,
		Insecure: cfg.TOSRPC.Insecure, ServerName: cfg.TOSRPC.ServerName,
		CAFile: cfg.TOSRPC.CAFile, ClientCertFile: cfg.TOSRPC.ClientCertFile,
		ClientKeyFile: cfg.TOSRPC.ClientKeyFile,
	})
	if err != nil {
		logger.Error("configure tos-protocol backend", "error", err)
		os.Exit(2)
	}
	defer backend.Close()

	readyCtx, cancelReady := context.WithTimeout(context.Background(), cfg.TOSRPC.Timeout)
	err = backend.CheckReady(readyCtx)
	cancelReady()
	if err != nil {
		logger.Error("tos-protocol backend is unavailable", "error", err)
		os.Exit(1)
	}

	gateway := &nativegateway.Server{
		Authorizer: nativegateway.NewTokenAuthorizer(cfg.NativeReadToken, cfg.NativeRelayToken),
		Backend:    backend,
	}
	mux := http.NewServeMux()
	path, handler := atosnativev1connect.NewNativeServiceHandler(gateway)
	mux.Handle(path, handler)
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", readinessHandler(backend, cfg.TOSRPC.Timeout))

	server := &http.Server{
		Addr: cfg.Addr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("ATOS Native gateway listening", "address", cfg.Addr)
		errCh <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		logger.Info("shutdown requested", "signal", sig.String())
	case serveErr := <-errCh:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("gateway stopped", "error", serveErr)
			os.Exit(1)
		}
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("gateway shutdown failed", "error", err)
	}
}

type readinessChecker interface{ CheckReady(context.Context) error }

func readinessHandler(checker readinessChecker, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if checker == nil || checker.CheckReady(ctx) != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
