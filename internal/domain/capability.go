// Package domain holds the ATOS business objects defined in atos-spec:
// Capability, Quote, Escrow, Receipt, Job and Account. Field names follow
// the JSON examples in ~/atos-spec/docs so REST/MCP payloads can (de)serialize
// with minimal adapter code.
package domain

import "time"

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

// TrustLevel mirrors docs/AGENT_CARD.md's signed-card ladder: a capability's
// trust starts self-asserted and strengthens as ATOS/tos-core corroborate it.
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
}

type SLA struct {
	TargetLatencyMS int64 `json:"target_latency_ms"`
	TimeoutMS       int64 `json:"timeout_ms"`
}

type Trust struct {
	Score float64    `json:"score"`
	Level TrustLevel `json:"level"`
}

// EndpointAdapterType is how tos-ai (or a human) actually fulfils the
// capability. ATOS only needs to know the adapter *category*, never the
// adapter's internal wiring — see docs/ARCHITECTURE.md's "hide execution
// internals" rule.
type EndpointAdapterType string

const (
	AdapterHTTP      EndpointAdapterType = "http"
	AdapterMCP       EndpointAdapterType = "mcp"
	AdapterA2A       EndpointAdapterType = "a2a"
	AdapterHuman     EndpointAdapterType = "human"
	AdapterTOSNative EndpointAdapterType = "tos-native"
)

type Capability struct {
	ID           string              `json:"id"`
	ProviderID   string              `json:"provider_id"`
	Name         string              `json:"name"`
	Description  string              `json:"description"`
	Version      string              `json:"version"`
	Tags         []string            `json:"tags,omitempty"`
	Modalities   []string            `json:"modalities,omitempty"`
	DeliveryMode DeliveryMode        `json:"delivery_mode"`
	InputSchema  map[string]any      `json:"input_schema"`
	OutputSchema map[string]any      `json:"output_schema"`
	Pricing      Pricing             `json:"pricing"`
	SLA          SLA                 `json:"sla"`
	Trust        Trust               `json:"trust"`
	AdapterType  EndpointAdapterType `json:"-"`
	Status       CapabilityStatus    `json:"status"`
	UpdatedAt    time.Time           `json:"updated_at"`
}
