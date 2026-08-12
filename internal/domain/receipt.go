package domain

import "time"

type ReceiptStatus string

const (
	ReceiptSettled              ReceiptStatus = "settled"
	ReceiptReleased             ReceiptStatus = "released"
	ReceiptDisputed             ReceiptStatus = "disputed"
	ReceiptSettledAfterDispute  ReceiptStatus = "settled_after_dispute"
	ReceiptReleasedAfterDispute ReceiptStatus = "released_after_dispute"
)

type Receipt struct {
	ID                       string          `json:"receipt_id"`
	QuoteID                  string          `json:"quote_id"`
	EscrowID                 string          `json:"escrow_id"`
	JobID                    string          `json:"job_id"`
	PrincipalID              string          `json:"-"`
	TrustMode                TrustMode       `json:"trust_mode"`
	ProofProfile             ProofProfile    `json:"proof_profile,omitempty"`
	Charged                  Money           `json:"charged"`
	Refunded                 Money           `json:"refunded"`
	Status                   ReceiptStatus   `json:"status"`
	ProofStatus              ProofCheckpoint `json:"proof_status"`
	NetworkProofRef          string          `json:"network_proof_ref,omitempty"`
	NetworkProofCheckpoint   uint64          `json:"network_proof_checkpoint,omitempty"`
	ProofOfServiceRef        string          `json:"proof_of_service_ref,omitempty"`
	ProofOfServiceDigest     string          `json:"proof_of_service_digest,omitempty"`
	ProofOfServiceCheckpoint uint64          `json:"proof_of_service_checkpoint,omitempty"`
	Finalized                bool            `json:"finalized"`
	FinalizedCheckpoint      uint64          `json:"finalized_checkpoint,omitempty"`
	ExecutionSignerID        string          `json:"execution_signer_id,omitempty"`
	SignerAuthorizationRef   string          `json:"signer_authorization_ref,omitempty"`
	InputCommitment          string          `json:"input_commitment,omitempty"`
	OutputCommitment         string          `json:"output_commitment,omitempty"`
	UsageCommitment          string          `json:"usage_commitment,omitempty"`
	CreatedAt                time.Time       `json:"created_at"`
}

type ExecutionResult string

const (
	ExecutionSuccess  ExecutionResult = "success"
	ExecutionFailed   ExecutionResult = "failed"
	ExecutionCanceled ExecutionResult = "canceled"
	ExecutionTimedOut ExecutionResult = "timed_out"
	ExecutionRejected ExecutionResult = "rejected"
)

type Usage struct {
	InputBytes      uint64 `json:"input_bytes,omitempty"`
	OutputBytes     uint64 `json:"output_bytes,omitempty"`
	InputTokens     uint64 `json:"input_tokens,omitempty"`
	OutputTokens    uint64 `json:"output_tokens,omitempty"`
	ExecutionMillis uint64 `json:"execution_millis,omitempty"`
}

// ExecutionReceipt is signed execution evidence. The Provider and the
// authorized execution signer may be different identities.
type ExecutionReceipt struct {
	ID                     string          `json:"receipt_id"`
	QuoteID                string          `json:"quote_id"`
	EscrowID               string          `json:"escrow_id"`
	JobID                  string          `json:"job_id"`
	PrincipalID            string          `json:"principal_id,omitempty"`
	ProviderID             string          `json:"provider_id"`
	CapabilityID           string          `json:"capability_id"`
	CapabilityVersion      string          `json:"capability_version"`
	TrustMode              TrustMode       `json:"trust_mode"`
	ProofProfile           ProofProfile    `json:"proof_profile,omitempty"`
	Result                 ExecutionResult `json:"result"`
	InputHash              string          `json:"input_commitment"`
	OutputHash             string          `json:"output_commitment"`
	UsageCommitment        string          `json:"usage_commitment,omitempty"`
	Artifacts              []Artifact      `json:"artifacts,omitempty"`
	Usage                  Usage           `json:"usage"`
	StartedAt              time.Time       `json:"started_at"`
	CompletedAt            time.Time       `json:"completed_at"`
	Cost                   Money           `json:"cost"`
	ExecutionSignerID      string          `json:"execution_signer_id"`
	SignerAuthorizationID  string          `json:"signer_authorization_id,omitempty"`
	SignerAuthorizationRef string          `json:"signer_authorization_ref,omitempty"`
	SignatureAlgorithm     string          `json:"signature_algorithm,omitempty"`
	Signature              string          `json:"signature"`
	// CanonicalEnvelope preserves the exact signed protocol wire bytes across
	// ATOS crashes. It is an internal durable recovery projection; authority
	// verification still happens in tos-protocol and on TOS.
	CanonicalEnvelope      string    `json:"canonical_envelope,omitempty"`
	NetworkProofRef        string    `json:"network_proof_ref,omitempty"`
	NetworkProofCheckpoint uint64    `json:"network_proof_checkpoint,omitempty"`
	ErrorCode              ErrorCode `json:"error_code,omitempty"`
}
