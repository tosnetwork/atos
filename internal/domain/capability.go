// Package domain holds the ATOS business objects defined in atos-spec.
package domain

import (
	"slices"
	"time"
)

type DeliveryMode string

const (
	DeliveryInstant     DeliveryMode = "instant"
	DeliveryAsync       DeliveryMode = "async"
	DeliveryInteractive DeliveryMode = "interactive"
)

type PricingModel string

const (
	PricingFree       PricingModel = "free"
	PricingFixed      PricingModel = "fixed"
	PricingPerUse     PricingModel = "per_use"
	PricingPerUnit    PricingModel = "per_unit"
	PricingMetered    PricingModel = "metered"
	PricingNegotiated PricingModel = "negotiated"
)

type CapabilityStatus string

const (
	CapabilityActive CapabilityStatus = "active"
	CapabilityPaused CapabilityStatus = "paused"
)

// TrustLevel is identity/attestation assurance. It is deliberately distinct
// from TrustMode, which is selected per Quote.
type TrustLevel string

const (
	TrustSelfAsserted TrustLevel = "self_asserted"
	TrustATOSVerified TrustLevel = "atos_verified"
	TrustTOSAttested  TrustLevel = "tos_attested"
)

type PriceHint struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type Pricing struct {
	Model     PricingModel `json:"model"`
	Unit      string       `json:"unit,omitempty"`
	PriceHint PriceHint    `json:"price_hint"`
	// MeteredRates optionally defines per-dimension unit prices for
	// PricingMetered/PricingPerUnit capabilities. See domain.MeteredRates.
	// Billing never reads this field directly at settlement time -- it is
	// frozen into the Quote at Quote-creation time (Quote.MeteredRates).
	MeteredRates *MeteredRates `json:"metered_rates,omitempty"`
}

type SLA struct {
	TargetLatencyMS int64 `json:"target_latency_ms"`
	TimeoutMS       int64 `json:"timeout_ms"`
}

type Trust struct {
	Score               float64    `json:"score"`
	Level               TrustLevel `json:"identity_assurance"`
	ProofOfServiceCount uint64     `json:"proof_of_service_count,omitempty"`
	LastUpdatedAt       *time.Time `json:"last_updated_at,omitempty"`
}

type EndpointAdapterType string

const (
	AdapterHTTP      EndpointAdapterType = "http"
	AdapterMCP       EndpointAdapterType = "mcp"
	AdapterA2A       EndpointAdapterType = "a2a"
	AdapterHuman     EndpointAdapterType = "human"
	AdapterTOSNative EndpointAdapterType = "tos-native"
)

type CapabilityBinding struct {
	Transport          EndpointAdapterType `json:"transport"`
	EndpointRef        string              `json:"endpoint_ref"`
	EligibleTrustModes []TrustMode         `json:"eligible_trust_modes"`
}

type Capability struct {
	ID                 string `json:"id"`
	CanonicalURI       string `json:"canonical_uri,omitempty"`
	ProviderID         string `json:"provider_id"`
	Name               string `json:"name"`
	Description        string `json:"description"`
	Version            string `json:"version"`
	ManifestCommitment string `json:"manifest_commitment,omitempty"`
	// Ownership is Phase 4A's ownership-anchoring projection
	// (docs/CAPABILITIES.md §1/§13). normalizeCapability defaults an empty
	// Status to OwnershipUnanchored -- Managed registration creates a
	// Capability before any TOS anchor exists; the field only becomes
	// OwnershipAnchored after CapabilityOwnershipService durably commits
	// this exact version's manifest/ownership through tos-protocol.
	Ownership                CapabilityOwnership `json:"ownership"`
	Tags                     []string            `json:"tags,omitempty"`
	Modalities               []string            `json:"modalities,omitempty"`
	DeliveryMode             DeliveryMode        `json:"delivery_mode"`
	InputSchema              map[string]any      `json:"input_schema"`
	OutputSchema             map[string]any      `json:"output_schema"`
	Pricing                  Pricing             `json:"pricing"`
	SLA                      SLA                 `json:"sla"`
	Trust                    Trust               `json:"trust_summary"`
	RequestedTrustModes      []TrustMode         `json:"requested_trust_modes"`
	SupportedTrustModes      []TrustMode         `json:"supported_trust_modes"`
	ModeSupport              ModeSupport         `json:"mode_support"`
	Bindings                 []CapabilityBinding `json:"bindings,omitempty"`
	RequiresArtifactTransfer bool                `json:"requires_artifact_transfer"`
	ArtifactInputFields      []string            `json:"artifact_input_fields,omitempty"`
	ArtifactOutputFields     []string            `json:"artifact_output_fields,omitempty"`
	// AdapterType remains an internal compatibility shortcut for the current
	// single-adapter Phase 0 runtime. Bindings are the public v0.2 contract.
	AdapterType EndpointAdapterType `json:"-"`
	Status      CapabilityStatus    `json:"status"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

func (c Capability) Supports(mode TrustMode) bool {
	return c.ModeSupport.Active(mode)
}

// SelectBinding picks the binding a Job executing under mode should freeze
// at creation time: the first binding whose EligibleTrustModes includes
// mode, falling back to the first binding at all if none declare
// eligibility explicitly (matching normalizeBindings' own permissive
// default at registration time). ok is false only when bindings is empty.
// Callers MUST freeze the result onto the Job at creation time (never
// re-resolve it live at execution time) -- see Job.Binding's doc comment.
func SelectBinding(bindings []CapabilityBinding, mode TrustMode) (CapabilityBinding, bool) {
	for _, b := range bindings {
		if slices.Contains(b.EligibleTrustModes, mode) {
			return b, true
		}
	}
	if len(bindings) > 0 {
		return bindings[0], true
	}
	return CapabilityBinding{}, false
}
