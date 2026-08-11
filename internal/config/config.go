// Package config loads ATOS's runtime configuration from the environment.
package config

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var nonNegativeDecimalPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)
var financialDomainIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

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

type FinancialBackend string

const (
	FinancialBackendDisabled FinancialBackend = "disabled"
	FinancialBackendBlnk     FinancialBackend = "blnk"
)

type FinancialConfig struct {
	Backend          FinancialBackend
	BlnkURL          string
	BlnkKey          string
	Timeout          time.Duration
	GatewayID        string
	NetworkID        string
	IssuanceLimit    string
	SignerURL        string
	SignerToken      string
	SigningKeyID     string
	SigningPublicKey string
	SigningAlgorithm string
	RetentionURL     string
	RetentionHMACKey string
	SealInterval     time.Duration
	BatchSize        int
}

type Config struct {
	Environment   Environment
	Addr          string
	DatabaseURL   string
	BlobDir       string
	PublicBaseURL string
	// TrustedProxyCIDRs lists the network ranges a forwarded-client-IP
	// header (X-Real-IP / X-Forwarded-For) may be trusted from --
	// currently consumed only by internal/httpapi's passkey rate limiter
	// (see httpapi.Server.clientIP). Empty by default: with no configured
	// trusted proxy, a forwarded header is never trusted and the raw TCP
	// peer address is used instead, since trusting an arbitrary caller-
	// suppliable header would let every anonymous caller pick their own
	// rate-limit bucket per request. Set this explicitly (and only to the
	// real load balancer/ingress/CDN's own address range) when ATOS is
	// deployed behind one -- otherwise every request behind that proxy
	// resolves to the SAME address and shares one rate-limit bucket,
	// which is a real, separate problem from the spoofing one this
	// defaults-empty behavior prevents.
	TrustedProxyCIDRs []string
	Auth              AuthConfig
	WebAuthn          WebAuthnConfig
	ManagedAccount    ManagedAccountConfig
	Financial         FinancialConfig
	TOSBackend        TOSBackend
	TOSRPC            TOSRPCConfig
	PayoutBackend     PayoutBackend
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
	financialTimeout, err := durationEnv("ATOS_BLNK_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	sealInterval, err := durationEnv("ATOS_FINANCIAL_SEAL_INTERVAL", time.Minute)
	if err != nil {
		return Config{}, err
	}
	batchSize, err := intEnv("ATOS_FINANCIAL_BATCH_SIZE", 4096)
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
		Environment:       environment,
		Addr:              envOr("ATOS_ADDR", ":8080"),
		DatabaseURL:       strings.TrimSpace(os.Getenv("ATOS_DATABASE_URL")),
		BlobDir:           envOr("ATOS_BLOB_DIR", "./data/blobs"),
		PublicBaseURL:     envOr("ATOS_PUBLIC_BASE_URL", "http://localhost:8080"),
		TrustedProxyCIDRs: splitAndTrim(strings.TrimSpace(os.Getenv("ATOS_TRUSTED_PROXY_CIDRS"))),
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
			RPOrigins:     splitAndTrim(strings.TrimSpace(os.Getenv("ATOS_WEBAUTHN_RP_ORIGINS"))),
		},
		ManagedAccount: ManagedAccountConfig{
			Currency:       strings.ToUpper(envOr("ATOS_MANAGED_CURRENCY", "USD")),
			InitialBalance: envOr("ATOS_MANAGED_INITIAL_BALANCE", "0.00"),
			PerCallLimit:   envOr("ATOS_MANAGED_PER_CALL_LIMIT", "2.00"),
			DailyLimit:     envOr("ATOS_MANAGED_DAILY_LIMIT", "20.00"),
		},
		Financial: FinancialConfig{
			Backend:          FinancialBackend(strings.ToLower(envOr("ATOS_FINANCIAL_BACKEND", string(FinancialBackendDisabled)))),
			BlnkURL:          strings.TrimSpace(os.Getenv("ATOS_BLNK_URL")),
			BlnkKey:          strings.TrimSpace(os.Getenv("ATOS_BLNK_KEY")),
			Timeout:          financialTimeout,
			GatewayID:        strings.TrimSpace(os.Getenv("ATOS_FINANCIAL_GATEWAY_ID")),
			NetworkID:        strings.TrimSpace(os.Getenv("ATOS_FINANCIAL_NETWORK_ID")),
			IssuanceLimit:    envOr("ATOS_FINANCIAL_ISSUANCE_LIMIT", "0.00"),
			SignerURL:        strings.TrimSpace(os.Getenv("ATOS_FINANCIAL_SIGNER_URL")),
			SignerToken:      os.Getenv("ATOS_FINANCIAL_SIGNER_TOKEN"),
			SigningKeyID:     strings.TrimSpace(os.Getenv("ATOS_FINANCIAL_SIGNING_KEY_ID")),
			SigningPublicKey: strings.TrimSpace(os.Getenv("ATOS_FINANCIAL_SIGNING_PUBLIC_KEY")),
			SigningAlgorithm: strings.ToLower(envOr("ATOS_FINANCIAL_SIGNING_ALGORITHM", "ed25519")),
			RetentionURL:     strings.TrimSpace(os.Getenv("ATOS_FINANCIAL_RETENTION_URL")),
			RetentionHMACKey: os.Getenv("ATOS_FINANCIAL_RETENTION_HMAC_KEY"),
			SealInterval:     sealInterval, BatchSize: batchSize,
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
	// Passkey auth is opt-in: cmd/api/main.go only constructs a webauthn.WebAuthn
	// instance when RPID is non-empty. Deliberately NOT defaulted from
	// PublicBaseURL -- PublicBaseURL always has a non-empty fallback
	// ("http://localhost:8080"), so defaulting RPID from it would make
	// RPID non-empty on every deployment, silently turning "opt-in" into
	// "always on" for any operator who never set ATOS_WEBAUTHN_RP_ID at
	// all. RPOrigins/RPDisplayName only get a default once RPID is
	// explicitly set, for the same reason (RPDisplayName's own default is
	// harmless either way -- it's inert unless RPID is also set).
	if cfg.WebAuthn.RPID != "" && len(cfg.WebAuthn.RPOrigins) == 0 {
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
	for _, cidr := range c.TrustedProxyCIDRs {
		if _, err := netip.ParsePrefix(cidr); err != nil {
			return fmt.Errorf("ATOS_TRUSTED_PROXY_CIDRS contains an invalid CIDR %q: %w", cidr, err)
		}
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
	switch c.Financial.Backend {
	case FinancialBackendDisabled:
	case FinancialBackendBlnk:
		if c.DatabaseURL == "" || c.Financial.BlnkURL == "" || c.Financial.GatewayID == "" || c.Financial.NetworkID == "" {
			return errors.New("ATOS_DATABASE_URL, ATOS_BLNK_URL, ATOS_FINANCIAL_GATEWAY_ID and ATOS_FINANCIAL_NETWORK_ID are required when ATOS_FINANCIAL_BACKEND=blnk")
		}
		if c.Financial.Timeout <= 0 || c.Financial.Timeout > 2*time.Minute {
			return errors.New("ATOS financial Blnk timeout is outside the allowed range")
		}
		if len(c.Financial.GatewayID) > 253 || len(c.Financial.NetworkID) > 128 ||
			!financialDomainIDPattern.MatchString(c.Financial.GatewayID) || !financialDomainIDPattern.MatchString(c.Financial.NetworkID) {
			return errors.New("ATOS_FINANCIAL_GATEWAY_ID or ATOS_FINANCIAL_NETWORK_ID is not a safe canonical domain identifier")
		}
		issuanceLimit, ok := new(big.Rat).SetString(c.Financial.IssuanceLimit)
		if !ok || issuanceLimit.Sign() < 0 || !nonNegativeDecimalPattern.MatchString(c.Financial.IssuanceLimit) {
			return errors.New("ATOS_FINANCIAL_ISSUANCE_LIMIT must be a non-negative decimal amount")
		}
		initialBalance, ok := new(big.Rat).SetString(c.ManagedAccount.InitialBalance)
		if !ok || initialBalance.Sign() < 0 || initialBalance.Sign() > 0 && issuanceLimit.Sign() <= 0 {
			return errors.New("a positive managed initial balance requires a positive ATOS_FINANCIAL_ISSUANCE_LIMIT")
		}
		if c.Financial.SealInterval <= 0 || c.Financial.SealInterval > 24*time.Hour || c.Financial.BatchSize < 1 || c.Financial.BatchSize > 4096 {
			return errors.New("ATOS financial seal interval or batch size is outside the allowed range")
		}
		if c.Financial.SigningAlgorithm != "ed25519" && c.Financial.SigningAlgorithm != "ecdsa_p256_sha256" {
			return errors.New("ATOS_FINANCIAL_SIGNING_ALGORITHM is unsupported")
		}
		if c.Financial.SigningPublicKey != "" {
			raw, err := base64.StdEncoding.Strict().DecodeString(c.Financial.SigningPublicKey)
			if err != nil {
				return errors.New("ATOS_FINANCIAL_SIGNING_PUBLIC_KEY is not strict base64")
			}
			if c.Financial.SigningAlgorithm == "ed25519" && len(raw) != ed25519.PublicKeySize {
				return errors.New("ATOS_FINANCIAL_SIGNING_PUBLIC_KEY is not an Ed25519 public key")
			}
			if c.Financial.SigningAlgorithm == "ecdsa_p256_sha256" {
				parsed, err := x509.ParsePKIXPublicKey(raw)
				key, ok := parsed.(*ecdsa.PublicKey)
				if err != nil || !ok || key.Curve != elliptic.P256() {
					return errors.New("ATOS_FINANCIAL_SIGNING_PUBLIC_KEY is not a P-256 PKIX public key")
				}
			}
		}
		if c.Financial.RetentionURL != "" && len(c.Financial.RetentionHMACKey) < 32 {
			return errors.New("ATOS_FINANCIAL_RETENTION_HMAC_KEY must contain at least 32 characters when WORM retention is configured")
		}
		if c.Financial.SignerURL != "" && len(c.Financial.SignerToken) < 32 {
			return errors.New("ATOS_FINANCIAL_SIGNER_TOKEN must contain at least 32 characters when an external signer is configured")
		}
	default:
		return fmt.Errorf("invalid ATOS_FINANCIAL_BACKEND %q (expected disabled or blnk)", c.Financial.Backend)
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
		if c.Financial.Backend != FinancialBackendBlnk {
			return errors.New("ATOS_FINANCIAL_BACKEND=blnk is required in production")
		}
		if c.Financial.BlnkKey == "" {
			return errors.New("ATOS_BLNK_KEY is required in production")
		}
		if c.Financial.SignerURL == "" || len(c.Financial.SignerToken) < 32 || c.Financial.SigningKeyID == "" || c.Financial.SigningPublicKey == "" || c.Financial.RetentionURL == "" || len(c.Financial.RetentionHMACKey) < 32 {
			return errors.New("authenticated external financial signer, pinned signing public key, key identity, authenticated WORM retention endpoint, and retention HMAC key are required in production")
		}
		for name, endpoint := range map[string]string{
			"ATOS_BLNK_URL": c.Financial.BlnkURL, "ATOS_FINANCIAL_SIGNER_URL": c.Financial.SignerURL,
			"ATOS_FINANCIAL_RETENTION_URL": c.Financial.RetentionURL,
		} {
			parsed, err := url.Parse(endpoint)
			if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
				return fmt.Errorf("%s must be an absolute HTTPS URL in production", name)
			}
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
// splitAndTrim splits a comma-separated value, trimming whitespace around
// each entry and dropping empty ones. An empty input yields a nil slice.
func splitAndTrim(raw string) []string {
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
