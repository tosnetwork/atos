package domain

import "time"

type EscrowStatus string

const (
	EscrowReserved EscrowStatus = "reserved"
	EscrowSettled  EscrowStatus = "settled"
	EscrowReleased EscrowStatus = "released"
	EscrowDisputed EscrowStatus = "disputed"
)

func (s EscrowStatus) Terminal() bool {
	return s == EscrowSettled || s == EscrowReleased
}

type Escrow struct {
	ID                    string                    `json:"escrow_id"`
	QuoteID               string                    `json:"quote_id"`
	JobID                 string                    `json:"job_id"`
	PrincipalID           string                    `json:"principal_id"`
	ProviderID            string                    `json:"provider_id"`
	CapabilityID          string                    `json:"capability_id"`
	CapabilityVersion     string                    `json:"capability_version"`
	TrustMode             TrustMode                 `json:"trust_mode"`
	ProofProfile          ProofProfile              `json:"proof_profile,omitempty"`
	Settlement            SettlementDescriptor      `json:"settlement"`
	Reserved              Money                     `json:"reserved"`
	Status                EscrowStatus              `json:"status"`
	NetworkProofRef       string                    `json:"network_proof_ref,omitempty"`
	TerminalProofRef      string                    `json:"terminal_proof_ref,omitempty"`
	QuoteCommitmentDigest string                    `json:"quote_commitment_digest,omitempty"`
	QuoteCommitmentRef    string                    `json:"quote_commitment_ref,omitempty"`
	ReservationDigest     string                    `json:"reservation_digest,omitempty"`
	ReservationActionID   string                    `json:"reservation_action_id,omitempty"`
	ContractCodeHash      string                    `json:"contract_code_hash,omitempty"`
	Finalized             bool                      `json:"finalized"`
	FinalizedCheckpoint   uint64                    `json:"finalized_checkpoint,omitempty"`
	OperationCheckpoint   EscrowOperationCheckpoint `json:"operation_checkpoint,omitempty"`
	ReleaseReason         string                    `json:"release_reason,omitempty"`
	ReleaseDigest         string                    `json:"release_digest,omitempty"`
	ReleaseActionID       string                    `json:"release_action_id,omitempty"`
	ReleaseRef            string                    `json:"release_ref,omitempty"`
	CreatedAt             time.Time                 `json:"created_at"`
	ExpiresAt             time.Time                 `json:"expires_at"`
	SettledAt             *time.Time                `json:"settled_at,omitempty"`
}

type EscrowOperationKind string

const (
	EscrowOperationReserve EscrowOperationKind = "reserve"
	EscrowOperationRelease EscrowOperationKind = "release"
)

type EscrowOperationCheckpoint string

const (
	EscrowOperationIntentPersisted     EscrowOperationCheckpoint = "intent_persisted"
	EscrowOperationReconciling         EscrowOperationCheckpoint = "reconciling"
	EscrowOperationAuthorityReserved   EscrowOperationCheckpoint = "authority_reserved"
	EscrowOperationAuthorityReleased   EscrowOperationCheckpoint = "authority_released"
	EscrowOperationProjectionPersisted EscrowOperationCheckpoint = "projection_persisted"
	EscrowOperationCompleted           EscrowOperationCheckpoint = "completed"
)

func (c EscrowOperationCheckpoint) Terminal() bool { return c == EscrowOperationCompleted }

func (c EscrowOperationCheckpoint) CanAdvance(next EscrowOperationCheckpoint, kind EscrowOperationKind) bool {
	if c == next {
		return true
	}
	if c.Terminal() {
		return false
	}
	order := map[EscrowOperationCheckpoint]int{
		EscrowOperationIntentPersisted: 1, EscrowOperationReconciling: 2,
		EscrowOperationAuthorityReserved: 3, EscrowOperationAuthorityReleased: 3,
		EscrowOperationProjectionPersisted: 4, EscrowOperationCompleted: 5,
	}
	if kind == EscrowOperationReserve && next == EscrowOperationAuthorityReleased {
		return false
	}
	if kind == EscrowOperationRelease && next == EscrowOperationAuthorityReserved {
		return false
	}
	return order[next] >= order[c] && order[next] != 0
}

type EscrowOperation struct {
	ID            string                    `json:"operation_id"`
	Kind          EscrowOperationKind       `json:"kind"`
	JobID         string                    `json:"job_id"`
	QuoteID       string                    `json:"quote_id"`
	PrincipalID   string                    `json:"principal_id"`
	RequestDigest string                    `json:"request_digest"`
	Escrow        Escrow                    `json:"escrow"`
	Checkpoint    EscrowOperationCheckpoint `json:"checkpoint"`
	LastErrorCode ErrorCode                 `json:"last_error_code,omitempty"`
	LastError     string                    `json:"last_error,omitempty"`
	CreatedAt     time.Time                 `json:"created_at"`
	UpdatedAt     time.Time                 `json:"updated_at"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
