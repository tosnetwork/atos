// Package config loads ATOS's runtime configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type TOSBackend string

const (
	TOSBackendMock TOSBackend = "mock"
	TOSBackendRPC  TOSBackend = "rpc"
)

type TOSRPCConfig struct {
	URL             string
	Token           string
	Insecure        bool
	Timeout         time.Duration
	MaxMessageBytes int
	ServerName      string
	CAFile          string
	ClientCertFile  string
	ClientKeyFile   string
}

type Config struct {
	Addr string
	// DatabaseURL selects the storage backend: empty uses the Phase 0
	// in-memory store (zero setup, non-durable); set it to use the
	// Phase 1 Postgres store (internal/store/postgres) instead. Run
	// cmd/migrate against the same URL first.
	DatabaseURL string
	// BlobDir is where internal/adapters/storage/local writes uploaded
	// file bytes. See ~/atos-spec/docs/ARTIFACTS.md.
	BlobDir string
	// PublicBaseURL is prefixed onto signed upload/download URLs so a
	// client can actually reach this server from outside the process —
	// it cannot be inferred from Addr alone (":8080" isn't a fetchable
	// host).
	PublicBaseURL string

	// TOSBackend is explicit. RPC failures never fall back to mock.
	TOSBackend TOSBackend
	TOSRPC     TOSRPCConfig
}

func Load() (Config, error) {
	addr := envOr("ATOS_ADDR", ":8080")
	blobDir := envOr("ATOS_BLOB_DIR", "./data/blobs")
	publicBaseURL := envOr("ATOS_PUBLIC_BASE_URL", "http://localhost:8080")
	backend := TOSBackend(strings.ToLower(envOr("ATOS_TOS_BACKEND", string(TOSBackendMock))))
	timeout, err := durationEnv("ATOS_TOS_RPC_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	maxBytes, err := intEnv("ATOS_TOS_RPC_MAX_MESSAGE_BYTES", 16<<20)
	if err != nil {
		return Config{}, err
	}
	insecure, err := boolEnv("ATOS_TOS_RPC_INSECURE", false)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Addr: addr, DatabaseURL: strings.TrimSpace(os.Getenv("ATOS_DATABASE_URL")),
		BlobDir: blobDir, PublicBaseURL: publicBaseURL,
		TOSBackend: backend,
		TOSRPC: TOSRPCConfig{
			URL:      strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_URL")),
			Token:    strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_TOKEN")),
			Insecure: insecure, Timeout: timeout, MaxMessageBytes: maxBytes,
			ServerName:     strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_SERVER_NAME")),
			CAFile:         strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CA_FILE")),
			ClientCertFile: strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CLIENT_CERT_FILE")),
			ClientKeyFile:  strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CLIENT_KEY_FILE")),
		},
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	switch c.TOSBackend {
	case TOSBackendMock:
		return nil
	case TOSBackendRPC:
		if c.TOSRPC.URL == "" || c.TOSRPC.Token == "" {
			return errors.New("ATOS_TOS_RPC_URL and ATOS_TOS_RPC_TOKEN are required when ATOS_TOS_BACKEND=rpc")
		}
		if c.TOSRPC.Timeout <= 0 || c.TOSRPC.Timeout > 15*time.Minute {
			return errors.New("ATOS_TOS_RPC_TIMEOUT is outside the allowed range")
		}
		if c.TOSRPC.MaxMessageBytes <= 0 || c.TOSRPC.MaxMessageBytes > 64<<20 {
			return errors.New("ATOS_TOS_RPC_MAX_MESSAGE_BYTES is outside the allowed range")
		}
		if (c.TOSRPC.ClientCertFile == "") != (c.TOSRPC.ClientKeyFile == "") {
			return errors.New("ATOS_TOS_RPC_CLIENT_CERT_FILE and ATOS_TOS_RPC_CLIENT_KEY_FILE must be configured together")
		}
		return nil
	default:
		return fmt.Errorf("invalid ATOS_TOS_BACKEND %q (expected mock or rpc)", c.TOSBackend)
	}
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func intEnv(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func boolEnv(name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}
