package domain

import "time"

type Price struct {
	Subtotal string `json:"subtotal"`
	Fees     string `json:"fees"`
	TotalMax string `json:"total_max"`
	Currency string `json:"currency"`
}

// Quote is the immutable boundary between discovery and financially
// committing execution. requested_trust_mode records caller intent while
// trust_mode is always concrete.
type Quote struct {
	ID                        string               `json:"quote_id"`
	CapabilityID              string               `json:"capability_id"`
	CapabilityVersion         string               `json:"capability_version"`
	ProviderID                string               `json:"provider_id"`
	PrincipalID               string               `json:"principal_id,omitempty"`
	RequestedTrustMode        RequestedTrustMode   `json:"requested_trust_mode"`
	TrustMode                 TrustMode            `json:"trust_mode"`
	ProofProfile              ProofProfile         `json:"proof_profile,omitempty"`
	Price                     Price                `json:"price"`
	Settlement                SettlementDescriptor `json:"settlement"`
	Proof                     ProofDescriptor      `json:"proof"`
	ExpiresAt                 time.Time            `json:"expires_at"`
	RequiresConfirmation      bool                 `json:"requires_confirmation"`
	TermsHash                 string               `json:"terms_hash"`
	DisputePolicyHash         string               `json:"dispute_policy_hash,omitempty"`
	ServiceQuoteID            string               `json:"service_quote_id,omitempty"`
	UnderlyingServiceQuoteRef string               `json:"underlying_service_quote_ref,omitempty"`
	InputSummaryCommitment    string               `json:"input_summary_commitment,omitempty"`
	ExecutionDeadline         time.Time            `json:"execution_deadline,omitempty"`
	CreatedAt                 time.Time            `json:"-"`
	// MeteredRates is frozen from the Capability's pricing at Quote-creation
	// time (nil if the Capability has none configured). Settlement billing
	// (internal/service/billing.go) reads only this frozen copy, never the
	// Capability's current live pricing -- see domain.MeteredRates.
	MeteredRates *MeteredRates `json:"metered_rates,omitempty"`
	// PricingModel is likewise frozen from the Capability's pricing at
	// Quote-creation time. Along with MeteredRates and Price, it is part of
	// the frozen pricing contract committed into TermsHash (see
	// internal/service/quote.go's Create) -- not merely recorded for audit
	// trail -- so two Quotes cannot share a TermsHash while differing in
	// how a Job would actually be charged.
	PricingModel PricingModel `json:"pricing_model,omitempty"`
	// Binding, InputSchema and OutputSchema are frozen from the Capability
	// at Quote-creation time via SelectBinding, exactly like MeteredRates/
	// PricingModel above. Job creation (internal/service/job.go's submit)
	// MUST use these frozen values, never re-derive them from the
	// Capability's live state -- an already-issued Quote/Job MUST continue
	// to use its frozen version/binding semantics after a later Capability
	// update (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.1.0's binding-
	// freeze rule). Binding is nil when the Capability has no transport
	// binding at all (the ordinary tos-native/human path), not as a signal
	// that this Quote predates this field -- InputSchema/OutputSchema are
	// unconditionally frozen for every Quote this package creates (a
	// Capability's schemas are required, never nil), so a persisted Quote
	// whose InputSchema is nil unambiguously predates this field instead
	// (see QuoteService.Get's legacy-Quote handling).
	//
	// These three fields are ATOS's own internal execution snapshot, not
	// part of the public Quote contract -- see PublicQuote's doc comment.
	// They are tagged for JSON persistence (the durable store round-trips
	// the whole struct through one JSON payload column) but a public
	// REST/MCP/A2A response MUST go through Quote.Public() instead of
	// serializing Quote directly, or they leak into a response
	// atos-spec docs/API.md never defined them for.
	Binding      *CapabilityBinding `json:"binding,omitempty"`
	InputSchema  map[string]any     `json:"input_schema,omitempty"`
	OutputSchema map[string]any     `json:"output_schema,omitempty"`
	// IdempotencyKey is the caller-supplied key this Quote was created
	// under, if any (empty for the many pre-Phase-3C call sites that never
	// set CreateQuoteInput.IdempotencyKey). Scoped by PrincipalID, exactly
	// like domain.Job.IdempotencyKey -- see store.Quotes.QuoteByIdempotencyKey
	// and QuoteService.Create's Reserve/Finish/Release wrapper. Internal
	// bookkeeping only, never part of the public Quote contract.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// IdempotencyRequestHash is QuoteService.Create's own requestHash
	// digest, persisted on the Quote itself (unlike IdempotencyKey, this
	// one is NOT tagged json:"-" -- it must actually round-trip through
	// the Postgres store's jsonb payload column). This exists so the
	// crash-recovery lookup path (QuoteByIdempotencyKey) can verify a
	// resumed/replayed call's content still matches what was originally
	// committed, rather than trusting the lookup blindly: the generic
	// store.Idempotency record that would normally hold this comparison
	// can be deleted out from under a genuinely-committed Quote if a
	// PRIOR attempt's own Finish call failed after PutQuote succeeded
	// (that attempt's deferred Release hard-deletes the idempotency
	// record) -- without this field, a LATER call reusing the same key
	// with genuinely different content would silently receive the old
	// Quote back instead of being rejected as a conflicting reuse.
	IdempotencyRequestHash     string                     `json:"idempotency_request_hash,omitempty"`
	NetworkID                  string                     `json:"network_id,omitempty"`
	CommitmentDomain           string                     `json:"commitment_domain,omitempty"`
	CommitmentVersion          string                     `json:"commitment_version,omitempty"`
	CommitmentCanonicalization string                     `json:"commitment_canonicalization,omitempty"`
	RequesterAgentID           string                     `json:"requester_agent_id,omitempty"`
	ManifestCommitment         string                     `json:"manifest_commitment,omitempty"`
	OwnershipRef               string                     `json:"ownership_ref,omitempty"`
	SignerAuthorizationID      string                     `json:"signer_authorization_id,omitempty"`
	SignerAuthorizationRef     string                     `json:"signer_authorization_ref,omitempty"`
	AssetDecimals              uint32                     `json:"asset_decimals,omitempty"`
	Commitment                 *QuoteCommitmentProjection `json:"commitment,omitempty"`
}

type QuoteCommitmentProjection struct {
	State               string `json:"state"`
	Network             string `json:"network"`
	Reference           string `json:"reference"`
	Digest              string `json:"digest"`
	Finalized           bool   `json:"finalized"`
	FinalizedCheckpoint uint64 `json:"finalized_checkpoint,omitempty"`
}

func (q Quote) Expired(now time.Time) bool {
	return !now.Before(q.ExpiresAt)
}

// PublicQuote is the atos-spec-normative public representation of a Quote
// (docs/API.md §3 "Quote Endpoints"). It deliberately omits
// Quote.Binding/InputSchema/OutputSchema -- ATOS's own internal frozen
// execution snapshot (see Quote.Binding's doc comment), never part of the
// public contract. These can be arbitrarily large (a full JSON Schema
// document) and returning them on every Quote response would be needless
// token/network cost for a caller that never needs them: Job responses
// already carry the same frozen values a Job actually executes against.
type PublicQuote struct {
	ID                        string                     `json:"quote_id"`
	CapabilityID              string                     `json:"capability_id"`
	CapabilityVersion         string                     `json:"capability_version"`
	ProviderID                string                     `json:"provider_id"`
	PrincipalID               string                     `json:"principal_id,omitempty"`
	RequestedTrustMode        RequestedTrustMode         `json:"requested_trust_mode"`
	TrustMode                 TrustMode                  `json:"trust_mode"`
	ProofProfile              ProofProfile               `json:"proof_profile,omitempty"`
	Price                     Price                      `json:"price"`
	Settlement                SettlementDescriptor       `json:"settlement"`
	Proof                     ProofDescriptor            `json:"proof"`
	ExpiresAt                 time.Time                  `json:"expires_at"`
	RequiresConfirmation      bool                       `json:"requires_confirmation"`
	TermsHash                 string                     `json:"terms_hash"`
	DisputePolicyHash         string                     `json:"dispute_policy_hash,omitempty"`
	ServiceQuoteID            string                     `json:"service_quote_id,omitempty"`
	UnderlyingServiceQuoteRef string                     `json:"underlying_service_quote_ref,omitempty"`
	InputSummaryCommitment    string                     `json:"input_summary_commitment,omitempty"`
	ExecutionDeadline         time.Time                  `json:"execution_deadline,omitempty"`
	MeteredRates              *MeteredRates              `json:"metered_rates,omitempty"`
	PricingModel              PricingModel               `json:"pricing_model,omitempty"`
	Commitment                *QuoteCommitmentProjection `json:"commitment,omitempty"`
}

// Public returns q's atos-spec-normative public representation -- every
// REST/MCP/A2A surface that returns a Quote to a caller MUST use this,
// never serialize Quote directly. See PublicQuote's doc comment.
func (q Quote) Public() PublicQuote {
	return PublicQuote{
		ID: q.ID, CapabilityID: q.CapabilityID, CapabilityVersion: q.CapabilityVersion,
		ProviderID: q.ProviderID, PrincipalID: q.PrincipalID,
		RequestedTrustMode: q.RequestedTrustMode, TrustMode: q.TrustMode, ProofProfile: q.ProofProfile,
		Price: q.Price, Settlement: q.Settlement, Proof: q.Proof,
		ExpiresAt: q.ExpiresAt, RequiresConfirmation: q.RequiresConfirmation,
		TermsHash: q.TermsHash, DisputePolicyHash: q.DisputePolicyHash,
		ServiceQuoteID: q.ServiceQuoteID, UnderlyingServiceQuoteRef: q.UnderlyingServiceQuoteRef,
		InputSummaryCommitment: q.InputSummaryCommitment, ExecutionDeadline: q.ExecutionDeadline,
		MeteredRates: q.MeteredRates, PricingModel: q.PricingModel,
		Commitment: q.Commitment,
	}
}

type QuoteCommitmentCheckpoint string

const (
	QuoteCommitmentIntentPersisted    QuoteCommitmentCheckpoint = "intent_persisted"
	QuoteCommitmentReconciling        QuoteCommitmentCheckpoint = "reconciling"
	QuoteCommitmentAuthorityCommitted QuoteCommitmentCheckpoint = "authority_committed"
	QuoteCommitmentCompleted          QuoteCommitmentCheckpoint = "completed"
)

type QuoteCommitmentOperation struct {
	QuoteID       string                    `json:"quote_id"`
	Quote         Quote                     `json:"quote"`
	ContentHash   string                    `json:"content_hash"`
	Checkpoint    QuoteCommitmentCheckpoint `json:"checkpoint"`
	FailureReason string                    `json:"failure_reason,omitempty"`
	CreatedAt     time.Time                 `json:"created_at"`
	UpdatedAt     time.Time                 `json:"updated_at"`
	CompletedAt   *time.Time                `json:"completed_at,omitempty"`
}
