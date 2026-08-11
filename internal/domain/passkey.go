// Package domain: human account authentication (passkey/WebAuthn) --
// atos-spec docs/AUTH.md's "Human Account Authentication (Passkey/WebAuthn)"
// section. This is the "who is this human" identity primitive Device
// Authorization's /activate consent page always assumed existed elsewhere
// (a "trusted login/reverse-proxy boundary") but never actually had. Modeled
// directly on tosnetwork/atos-aidrop's proven tos-wallet-web implementation.
package domain

import "time"

// PasskeyAccount is a first-party atos.im human account. PrincipalID is the
// SAME prn_... namespace Device Authorization already uses -- a passkey
// account is not a second identity system, just a second way to obtain a
// principal (see service.PasskeyService.FinishRegistration and
// auth.Service.IssueForPrincipal).
type PasskeyAccount struct {
	PrincipalID string `json:"principal_id"`
	// DisplayHandle is the public, shareable handle shown by the browser's
	// own passkey picker when an account has more than one saved
	// credential -- never the PrincipalID itself, which is an internal
	// identifier, not something meant to be recognizable to a human.
	DisplayHandle string    `json:"display_handle"`
	CreatedAt     time.Time `json:"created_at"`
}

// WebAuthnCredentialRecord is one durably attested passkey. PublicKey is
// stored in plaintext -- a WebAuthn public key is not secret material (the
// matching private key never leaves the authenticator), so encrypting it at
// rest would add no real confidentiality.
type WebAuthnCredentialRecord struct {
	ID                string     `json:"id"`
	PrincipalID       string     `json:"principal_id"`
	CredentialID      []byte     `json:"-"`
	PublicKey         []byte     `json:"-"`
	AttestationType   string     `json:"-"`
	AttestationFormat string     `json:"-"`
	Transports        string     `json:"-"`
	AAGUID            []byte     `json:"-"`
	Flags             byte       `json:"-"`
	SignCount         uint32     `json:"-"`
	CloneWarning      bool       `json:"-"`
	BackupEligible    bool       `json:"-"`
	BackupState       bool       `json:"-"`
	Nickname          string     `json:"nickname"`
	CreatedAt         time.Time  `json:"created_at"`
	LastUsedAt        *time.Time `json:"last_used_at,omitempty"`
}

// WebAuthnCeremonyPurpose distinguishes the three ceremony shapes a
// begin/finish pair can carry -- see WebAuthnCeremony's doc comment for why
// PrincipalID is nullable for two of the three.
type WebAuthnCeremonyPurpose string

const (
	CeremonyRegistration WebAuthnCeremonyPurpose = "registration"
	CeremonyLogin        WebAuthnCeremonyPurpose = "login"
	CeremonySignup       WebAuthnCeremonyPurpose = "signup"
)

// WebAuthnCeremony is the short-lived challenge state exchanged between a
// begin/finish pair. PrincipalID is empty for CeremonyLogin (a discoverable
// login resolves the account from the assertion response itself, never from
// the request that started the ceremony) and CeremonySignup (no account row
// exists yet at begin time -- the future account's identity travels inside
// SessionData instead, see service.signupCeremonyPayload). Only
// CeremonyRegistration (adding an additional passkey to an ALREADY
// authenticated account) has PrincipalID set at begin time.
type WebAuthnCeremony struct {
	ID          string
	PrincipalID string
	Purpose     WebAuthnCeremonyPurpose
	// SessionData is the go-webauthn library's own opaque challenge state
	// (marshaled JSON), or -- for CeremonySignup only -- a superset that
	// additionally carries the not-yet-created account's identity. Never
	// interpreted by anything other than internal/service/passkey.go.
	SessionData []byte
	ExpiresAt   time.Time
	CreatedAt   time.Time
}
