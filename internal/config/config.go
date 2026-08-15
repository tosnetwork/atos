// Package config loads the Native-only ATOS gateway configuration.
package config

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
	Addr             string
	PublicBaseURL    string
	NativeReadToken  string
	NativeRelayToken string
	TOSRPC           TOSRPCConfig
	Catalog          CatalogConfig
}

type CatalogConfig struct {
	Directory        string
	NetworkID        string
	GenesisRootHash  string
	GenesisFileHash  string
	RegistryCodeHash string
	MaxEntries       uint32
}

func Load() (Config, error) {
	timeout, err := durationEnv("ATOS_TOS_RPC_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	maxBytes, err := intEnv("ATOS_TOS_RPC_MAX_MESSAGE_BYTES", 16<<20)
	if err != nil {
		return Config{}, err
	}
	maxCatalogEntries, err := intEnv("ATOS_CAPABILITY_CATALOG_MAX_ENTRIES", 10_000)
	if err != nil || maxCatalogEntries <= 0 {
		return Config{}, errors.New("ATOS_CAPABILITY_CATALOG_MAX_ENTRIES must be a positive integer")
	}
	insecure, err := boolEnv("ATOS_TOS_RPC_INSECURE", false)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Addr:             envOr("ATOS_ADDR", ":8080"),
		PublicBaseURL:    strings.TrimRight(strings.TrimSpace(os.Getenv("ATOS_PUBLIC_BASE_URL")), "/"),
		NativeReadToken:  strings.TrimSpace(os.Getenv("ATOS_NATIVE_READ_TOKEN")),
		NativeRelayToken: strings.TrimSpace(os.Getenv("ATOS_NATIVE_RELAY_TOKEN")),
		TOSRPC: TOSRPCConfig{
			URL: envOr("ATOS_TOS_RPC_URL", "https://127.0.0.1:9443"), Token: strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_TOKEN")),
			Insecure: insecure, Timeout: timeout, MaxMessageBytes: maxBytes,
			ServerName: strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_SERVER_NAME")), CAFile: strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CA_FILE")),
			ClientCertFile: strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CLIENT_CERT_FILE")), ClientKeyFile: strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CLIENT_KEY_FILE")),
		},
		Catalog: CatalogConfig{Directory: strings.TrimSpace(os.Getenv("ATOS_CAPABILITY_CATALOG_DIRECTORY")),
			NetworkID:        strings.TrimSpace(os.Getenv("ATOS_NATIVE_NETWORK_ID")),
			GenesisRootHash:  strings.TrimSpace(os.Getenv("ATOS_NATIVE_GENESIS_ROOT_HASH")),
			GenesisFileHash:  strings.TrimSpace(os.Getenv("ATOS_NATIVE_GENESIS_FILE_HASH")),
			RegistryCodeHash: strings.TrimSpace(os.Getenv("ATOS_NATIVE_REGISTRY_CODE_HASH")), MaxEntries: uint32(maxCatalogEntries)},
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Addr) == "" {
		return errors.New("ATOS_ADDR is required")
	}
	publicURL, err := url.Parse(c.PublicBaseURL)
	if err != nil || publicURL == nil || publicURL.Scheme == "" || publicURL.Host == "" || publicURL.User != nil || publicURL.RawQuery != "" || publicURL.Fragment != "" || publicURL.Path != "" {
		return errors.New("ATOS_PUBLIC_BASE_URL must be an absolute origin without path, credentials, query, or fragment")
	}
	if publicURL.Scheme != "https" && !(publicURL.Scheme == "http" && (publicURL.Hostname() == "127.0.0.1" || publicURL.Hostname() == "localhost" || publicURL.Hostname() == "::1")) {
		return errors.New("ATOS_PUBLIC_BASE_URL must use HTTPS outside loopback development")
	}
	if c.NativeReadToken == "" || c.NativeRelayToken == "" {
		return errors.New("ATOS_NATIVE_READ_TOKEN and ATOS_NATIVE_RELAY_TOKEN are required")
	}
	if c.NativeReadToken == c.NativeRelayToken {
		return errors.New("Native read and relay tokens must be distinct")
	}
	if c.TOSRPC.Token == "" {
		return errors.New("ATOS_TOS_RPC_TOKEN is required")
	}
	parsed, err := url.Parse(c.TOSRPC.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return errors.New("ATOS_TOS_RPC_URL must be an absolute http(s) URL")
	}
	if parsed.Scheme == "http" && !c.TOSRPC.Insecure {
		return errors.New("plaintext tos-protocol RPC requires ATOS_TOS_RPC_INSECURE=true")
	}
	if c.TOSRPC.Timeout <= 0 || c.TOSRPC.Timeout > 15*time.Minute {
		return errors.New("ATOS_TOS_RPC_TIMEOUT is outside bounds")
	}
	if c.TOSRPC.MaxMessageBytes <= 0 || c.TOSRPC.MaxMessageBytes > 64<<20 {
		return errors.New("ATOS_TOS_RPC_MAX_MESSAGE_BYTES is outside bounds")
	}
	if (c.TOSRPC.ClientCertFile == "") != (c.TOSRPC.ClientKeyFile == "") {
		return errors.New("tos-protocol client certificate and key must be configured together")
	}
	if !filepath.IsAbs(c.Catalog.Directory) || filepath.Clean(c.Catalog.Directory) != c.Catalog.Directory {
		return errors.New("ATOS_CAPABILITY_CATALOG_DIRECTORY must be absolute and clean")
	}
	if c.Catalog.NetworkID == "" || c.Catalog.GenesisRootHash == "" || c.Catalog.GenesisFileHash == "" || c.Catalog.RegistryCodeHash == "" {
		return errors.New("Native network/genesis/Registry identity is required for Capability discovery")
	}
	if c.Catalog.MaxEntries == 0 || c.Catalog.MaxEntries > 1_000_000 {
		return errors.New("Capability catalog entry bound is invalid")
	}
	return nil
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
		return 0, errors.New(name + " must be a duration")
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
		return 0, errors.New(name + " must be an integer")
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
		return false, errors.New(name + " must be a boolean")
	}
	return parsed, nil
}
