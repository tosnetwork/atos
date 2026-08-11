// Package config loads ATOS's runtime configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Environment string

const (
	EnvironmentDevelopment Environment = "development"
	EnvironmentProduction  Environment = "production"
)

type TOSBackend string

const (
	TOSBackendMock TOSBackend = "mock"
	TOSBackendRPC  TOSBackend = "rpc"
)

// PayoutBackend selects the external side effect provider earnings actually
// pay out through. PayoutBackendDisabled is the default and the only safe
// choice absent a real payout rail: earnings still mature to Available, but
// nothing ever attempts an external payout, so the ledger can never mark a
// provider "paid" when no funds actually moved. PayoutBackendMock is an
// explicit opt-in for development/test only -- it never moves real funds
// either, but it does drive earnings through to Paid, so it must never be
// the default in a real deployment.
type PayoutBackend string

const (
	PayoutBackendDisabled PayoutBackend = "disabled"
	PayoutBackendMock     PayoutBackend = "mock"
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

type AuthConfig struct {
	AutoApprove   bool
	StatePath     string
	ApprovalToken string
	// AdminApprovalToken gates approval of a Device Authorization grant
	// requesting any scope in auth.RequiresAdminApproval's set, in
	// addition to (not instead of) ApprovalToken -- see
	// internal/httpapi/auth.go's DecideDevice callers. Deliberately no
	// startup-validation requirement like ApprovalToken's: leaving it
	// unset means admin-scoped grants can simply never be approved (a
	// safe fail-closed default), not a broken service.
	AdminApprovalToken string
	TokenTTL           time.Duration
	DeviceTTL          time.Duration
	PollInterval       time.Duration
}

// WebAuthnConfig backs the passkey/WebAuthn human-account authentication
// system (atos-spec docs/AUTH.md's "Human Account Authentication
// (Passkey/WebAuthn)" section) -- deliberately separate from AuthConfig,
// which governs the pre-existing Device Authorization mechanism these
// passkey ceremonies issue tokens through (see
// auth.Service.IssueForPrincipal), not a parallel token system.
type WebAuthnConfig struct {
	RPID          string
	RPDisplayName string
	RPOrigins     []string
}

type ManagedAccountConfig struct {
	Currency       string
	InitialBalance string
	PerCallLimit   string
	DailyLimit     string
}

type Config struct {
	Environment    Environment
	Addr           string
	DatabaseURL    string
	BlobDir        string
	PublicBaseURL  string
	Auth           AuthConfig
	WebAuthn       WebAuthnConfig
	ManagedAccount ManagedAccountConfig
	TOSBackend     TOSBackend
	TOSRPC         TOSRPCConfig
	PayoutBackend  PayoutBackend
	// RemoteThirdPartyExecution routes http/mcp/a2a Job execution through
	// tos-protocol/tos-ai (see internal/adapters/tosai/dispatch.
	// WithRemoteThirdPartyExecution's doc comment) instead of this process
	// dialing the provider endpoint directly. Defaults to false so
	// development/test can run without that stack deployed, but
	// Validate() requires it to be true in production -- see atos-spec
	// docs/THIRD_PARTY_EXECUTION_PLANE.md §7.1.1's placement rule.
	RemoteThirdPartyExecution bool
}

func Load() (Config, error) {
	environment := Environment(strings.ToLower(envOr("ATOS_ENV", string(EnvironmentDevelopment))))
	production := environment == EnvironmentProduction
	autoApproveDefault := !production
	autoApprove, err := boolEnv("ATOS_AUTH_AUTO_APPROVE", autoApproveDefault)
	if err != nil {
		return Config{}, err
	}
	tokenTTL, err := durationEnv("ATOS_AUTH_TOKEN_TTL", time.Hour)
	if err != nil {
		return Config{}, err
	}
	deviceTTL, err := durationEnv("ATOS_AUTH_DEVICE_TTL", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := durationEnv("ATOS_AUTH_POLL_INTERVAL", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
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
	remoteThirdParty, err := boolEnv("ATOS_REMOTE_THIRD_PARTY_EXECUTION", false)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		Environment:   environment,
		Addr:          envOr("ATOS_ADDR", ":8080"),
		DatabaseURL:   strings.TrimSpace(os.Getenv("ATOS_DATABASE_URL")),
		BlobDir:       envOr("ATOS_BLOB_DIR", "./data/blobs"),
		PublicBaseURL: envOr("ATOS_PUBLIC_BASE_URL", "http://localhost:8080"),
		Auth: AuthConfig{
			AutoApprove:        autoApprove,
			StatePath:          strings.TrimSpace(os.Getenv("ATOS_AUTH_STATE_PATH")),
			ApprovalToken:      strings.TrimSpace(os.Getenv("ATOS_APPROVAL_TOKEN")),
			AdminApprovalToken: strings.TrimSpace(os.Getenv("ATOS_ADMIN_APPROVAL_TOKEN")),
			TokenTTL:           tokenTTL, DeviceTTL: deviceTTL, PollInterval: pollInterval,
		},
		WebAuthn: WebAuthnConfig{
			RPID:          strings.TrimSpace(os.Getenv("ATOS_WEBAUTHN_RP_ID")),
			RPDisplayName: envOr("ATOS_WEBAUTHN_RP_NAME", "ATOS"),
			RPOrigins:     webAuthnOrigins(strings.TrimSpace(os.Getenv("ATOS_WEBAUTHN_RP_ORIGINS"))),
		},
		ManagedAccount: ManagedAccountConfig{
			Currency:       strings.ToUpper(envOr("ATOS_MANAGED_CURRENCY", "USD")),
			InitialBalance: envOr("ATOS_MANAGED_INITIAL_BALANCE", "25.00"),
			PerCallLimit:   envOr("ATOS_MANAGED_PER_CALL_LIMIT", "2.00"),
			DailyLimit:     envOr("ATOS_MANAGED_DAILY_LIMIT", "20.00"),
		},
		TOSBackend: TOSBackend(strings.ToLower(envOr("ATOS_TOS_BACKEND", string(TOSBackendMock)))),
		TOSRPC: TOSRPCConfig{
			URL:      strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_URL")),
			Token:    strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_TOKEN")),
			Insecure: insecure, Timeout: timeout, MaxMessageBytes: maxBytes,
			ServerName:     strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_SERVER_NAME")),
			CAFile:         strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CA_FILE")),
			ClientCertFile: strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CLIENT_CERT_FILE")),
			ClientKeyFile:  strings.TrimSpace(os.Getenv("ATOS_TOS_RPC_CLIENT_KEY_FILE")),
		},
		PayoutBackend:             PayoutBackend(strings.ToLower(envOr("ATOS_PAYOUT_BACKEND", string(PayoutBackendDisabled)))),
		RemoteThirdPartyExecution: remoteThirdParty,
	}
	// WebAuthn RPID/RPOrigins default from PublicBaseURL when unset, the
	// same way atos-aidrop derives its own RP config from PublicHostname/
	// PublicURL -- passkey auth is opt-in (cmd/api/main.go skips wiring it
	// entirely when RPID is empty), so this only matters once an operator
	// sets ATOS_WEBAUTHN_RP_ID or ATOS_WEBAUTHN_RP_ORIGINS explicitly, or
	// relies on this default alongside them.
	if cfg.WebAuthn.RPID == "" {
		if parsed, err := url.Parse(cfg.PublicBaseURL); err == nil {
			cfg.WebAuthn.RPID = parsed.Hostname()
		}
	}
	if len(cfg.WebAuthn.RPOrigins) == 0 {
		cfg.WebAuthn.RPOrigins = []string{cfg.PublicBaseURL}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	switch c.Environment {
	case EnvironmentDevelopment, EnvironmentProduction:
	default:
		return fmt.Errorf("invalid ATOS_ENV %q", c.Environment)
	}
	parsedBase, err := url.Parse(c.PublicBaseURL)
	if err != nil || parsedBase.Scheme == "" || parsedBase.Host == "" || parsedBase.RawQuery != "" || parsedBase.Fragment != "" {
		return errors.New("ATOS_PUBLIC_BASE_URL must be an absolute URL without query or fragment")
	}
	if parsedBase.Path != "" && parsedBase.Path != "/" {
		return errors.New("ATOS_PUBLIC_BASE_URL must not contain a path prefix")
	}
	if c.Auth.TokenTTL <= 0 || c.Auth.TokenTTL > 30*24*time.Hour ||
		c.Auth.DeviceTTL <= 0 || c.Auth.DeviceTTL > time.Hour ||
		c.Auth.PollInterval < time.Second || c.Auth.PollInterval > time.Minute {
		return errors.New("ATOS authorization durations are outside the allowed range")
	}
	if c.Auth.StatePath != "" && (!filepath.IsAbs(c.Auth.StatePath) || filepath.Clean(c.Auth.StatePath) != c.Auth.StatePath) {
		return errors.New("ATOS_AUTH_STATE_PATH must be an absolute clean path")
	}
	if !c.Auth.AutoApprove && len(c.Auth.ApprovalToken) < 32 {
		return errors.New("ATOS_APPROVAL_TOKEN must contain at least 32 characters when automatic approval is disabled")
	}
	if strings.TrimSpace(c.ManagedAccount.Currency) == "" || len(c.ManagedAccount.Currency) > 12 {
		return errors.New("ATOS_MANAGED_CURRENCY is invalid")
	}

	switch c.TOSBackend {
	case TOSBackendMock:
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
	default:
		return fmt.Errorf("invalid ATOS_TOS_BACKEND %q (expected mock or rpc)", c.TOSBackend)
	}

	switch c.PayoutBackend {
	case PayoutBackendDisabled, PayoutBackendMock:
	default:
		return fmt.Errorf("invalid ATOS_PAYOUT_BACKEND %q (expected disabled or mock)", c.PayoutBackend)
	}

	if c.Environment == EnvironmentProduction {
		if c.DatabaseURL == "" {
			return errors.New("ATOS_DATABASE_URL is required in production")
		}
		if parsedBase.Scheme != "https" {
			return errors.New("ATOS_PUBLIC_BASE_URL must use HTTPS in production")
		}
		if c.Auth.AutoApprove {
			return errors.New("ATOS_AUTH_AUTO_APPROVE must be false in production")
		}
		if c.Auth.StatePath == "" {
			return errors.New("ATOS_AUTH_STATE_PATH is required in production")
		}
		if c.TOSBackend != TOSBackendRPC {
			return errors.New("ATOS_TOS_BACKEND=rpc is required in production")
		}
		if !c.RemoteThirdPartyExecution {
			return errors.New("ATOS_REMOTE_THIRD_PARTY_EXECUTION=true is required in production " +
				"(see atos-spec docs/THIRD_PARTY_EXECUTION_PLANE.md §7.1.1: production must not let this " +
				"Gateway process dial a third-party HTTP/MCP/A2A provider endpoint itself)")
		}
		if c.PayoutBackend == PayoutBackendMock {
			return errors.New("ATOS_PAYOUT_BACKEND=mock never moves real funds and must not be used in production")
		}
	}
	return nil
}

// webAuthnOrigins splits a comma-separated ATOS_WEBAUTHN_RP_ORIGINS value,
// trimming whitespace around each entry. An empty input yields an empty
// slice -- the caller defaults it from PublicBaseURL afterward.
func webAuthnOrigins(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
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
