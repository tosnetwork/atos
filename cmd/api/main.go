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

	"connectrpc.com/connect"
	"github.com/tosnetwork/atos/internal/adapters/tosprotocol"
	"github.com/tosnetwork/atos/internal/config"
	"github.com/tosnetwork/atos/internal/nativegateway"
	nativev1 "github.com/tosnetwork/tos-protocol/gen/atos/native/v1"
	"github.com/tosnetwork/tos-protocol/gen/atos/native/v1/atosnativev1connect"
	"github.com/tosnetwork/tos-protocol/pkg/capabilitycatalog"
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
	catalog, err := capabilitycatalog.New(capabilitycatalog.Config{
		Directory: cfg.Catalog.Directory, Resolver: catalogBackend{backend},
		Network: &nativev1.NetworkDomain{NetworkId: cfg.Catalog.NetworkID,
			GenesisRootHash: cfg.Catalog.GenesisRootHash, GenesisFileHash: cfg.Catalog.GenesisFileHash},
		RegistryCodeHash: cfg.Catalog.RegistryCodeHash, CallerID: "atos-capability-catalog",
		ResolveTimeout: cfg.TOSRPC.Timeout, MaxEntries: cfg.Catalog.MaxEntries,
	})
	if err != nil {
		logger.Error("configure Capability catalog", "error", err)
		os.Exit(2)
	}

	readyCtx, cancelReady := context.WithTimeout(context.Background(), cfg.TOSRPC.Timeout)
	err = backend.CheckReady(readyCtx)
	cancelReady()
	if err != nil {
		logger.Error("tos-protocol backend is unavailable", "error", err)
		os.Exit(1)
	}

	gateway := &nativegateway.Server{
		Authorizer: nativegateway.NewTokenAuthorizer(cfg.NativeReadToken, cfg.NativeRelayToken),
		Backend:    backend, Catalog: catalog,
	}
	mux := http.NewServeMux()
	path, handler := atosnativev1connect.NewNativeServiceHandler(gateway)
	mux.Handle(path, handler)
	discoveryPath, discoveryHandler := atosnativev1connect.NewCapabilityDiscoveryServiceHandler(gateway)
	mux.Handle(discoveryPath, discoveryHandler)
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

type catalogBackend struct{ client *toprotocol.Client }

func (b catalogBackend) ResolveNativeState(ctx context.Context, request *nativev1.ResolveNativeStateRequest) (*nativev1.ResolveNativeStateResponse, error) {
	response, err := b.client.ResolveNativeState(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, err
	}
	return response.Msg, nil
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
