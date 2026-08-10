package domain

import "fmt"

// RequestedTrustMode is accepted only before Quote creation. Auto is a
// selection policy and must never be persisted as the concrete mode of a
// Quote, Job, Escrow, Receipt, or settlement.
type RequestedTrustMode string

const (
	RequestedTrustManaged  RequestedTrustMode = "managed"
	RequestedTrustVerified RequestedTrustMode = "verified"
	RequestedTrustNative   RequestedTrustMode = "native"
	RequestedTrustAuto     RequestedTrustMode = "auto"
)

func (m RequestedTrustMode) Valid() bool {
	switch m {
	case RequestedTrustManaged, RequestedTrustVerified, RequestedTrustNative, RequestedTrustAuto:
		return true
	default:
		return false
	}
}

// TrustMode is the concrete, immutable transaction mode selected by a Quote.
type TrustMode string

const (
	TrustModeManaged  TrustMode = "managed"
	TrustModeVerified TrustMode = "verified"
	TrustModeNative   TrustMode = "native"
)

func (m TrustMode) Valid() bool {
	switch m {
	case TrustModeManaged, TrustModeVerified, TrustModeNative:
		return true
	default:
		return false
	}
}

func (m RequestedTrustMode) Concrete() (TrustMode, bool) {
	switch m {
	case RequestedTrustManaged:
		return TrustModeManaged, true
	case RequestedTrustVerified:
		return TrustModeVerified, true
	case RequestedTrustNative:
		return TrustModeNative, true
	default:
		return "", false
	}
}

// ProofProfile gives a portable minimum meaning to Verified and Native.
type ProofProfile string

const (
	ProofProfileNone          ProofProfile = ""
	ProofProfileTOSVerifiedV1 ProofProfile = "tos_verified_v1"
	ProofProfileTOSNativeV1   ProofProfile = "tos_native_v1"
)

func StandardProofProfile(mode TrustMode) ProofProfile {
	switch mode {
	case TrustModeVerified:
		return ProofProfileTOSVerifiedV1
	case TrustModeNative:
		return ProofProfileTOSNativeV1
	default:
		return ProofProfileNone
	}
}

// ValidateCommittedTrust validates a trust mode/profile pair after Quote
// resolution. Auto is not representable as TrustMode; Managed must carry no
// network proof profile, while Verified and Native must carry their normative
// v0.2 profiles. This helper is used at every committed persistence boundary.
func ValidateCommittedTrust(mode TrustMode, profile ProofProfile) error {
	if !mode.Valid() {
		return fmt.Errorf("invalid committed trust_mode %q", mode)
	}
	want := StandardProofProfile(mode)
	if profile != want {
		return fmt.Errorf("proof_profile %q does not match trust_mode %q (want %q)", profile, mode, want)
	}
	return nil
}

type ModeSupportStatus string

const (
	ModeSupportRequested   ModeSupportStatus = "requested"
	ModeSupportPending     ModeSupportStatus = "pending"
	ModeSupportActive      ModeSupportStatus = "active"
	ModeSupportSuspended   ModeSupportStatus = "suspended"
	ModeSupportUnsupported ModeSupportStatus = "unsupported"
)

type ModeSupportEntry struct {
	Status       ModeSupportStatus `json:"status"`
	ProofProfile ProofProfile      `json:"proof_profile,omitempty"`
	Reason       string            `json:"reason,omitempty"`
}

// ModeSupport is keyed only by concrete mode names. "auto" is invalid here.
type ModeSupport map[TrustMode]ModeSupportEntry

func (m ModeSupport) Entry(mode TrustMode) ModeSupportEntry {
	if m == nil {
		return ModeSupportEntry{Status: ModeSupportUnsupported}
	}
	entry, ok := m[mode]
	if !ok {
		return ModeSupportEntry{Status: ModeSupportUnsupported}
	}
	return entry
}

func (m ModeSupport) Active(mode TrustMode) bool {
	return m.Entry(mode).Status == ModeSupportActive
}

func (m ModeSupport) ActiveModes() []TrustMode {
	ordered := []TrustMode{TrustModeManaged, TrustModeVerified, TrustModeNative}
	out := make([]TrustMode, 0, len(ordered))
	for _, mode := range ordered {
		if m.Active(mode) {
			out = append(out, mode)
		}
	}
	return out
}

// with returns a shallow copy of m with mode set to entry -- it NEVER
// mutates the receiver. This is why AdvanceToPending/Suspend/Activate are
// safe to call on a ModeSupport a caller obtained from a store's Get and
// has not yet Put back: the memory store's Get returns a domain.Capability
// by value, but Go copies a struct's map fields by reference, so the
// returned Capability's ModeSupport is still the SAME map object the
// store holds internally. Mutating that map in place (the previous
// implementation) is therefore a genuine, unsynchronized concurrent
// read/write on the store's own state -- proven by go test -race, not
// theoretical -- the instant a second goroutine calls Get and reads
// ModeSupport (e.g. via ActiveModes) while this one is still mid-mutation,
// before its own Put ever runs. Copy-on-write closes this at the source:
// the store's map is never touched until the caller's own Put swaps in an
// entirely new Capability value under the store's lock.
func (m ModeSupport) with(mode TrustMode, entry ModeSupportEntry) ModeSupport {
	next := make(ModeSupport, len(m)+1)
	for k, v := range m {
		next[k] = v
	}
	next[mode] = entry
	return next
}

// AdvanceToPending applies the `requested -> pending` transition from
// atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.2.0's frozen matrix, whose
// sole authority is the readiness pipeline recording a first evidence
// cycle (health check or certification attempt) for the Capability's
// current version. A no-op (not an error) for any other current status --
// only `requested` legally becomes `pending` this way. Safe to call on a
// nil ModeSupport (Entry already treats it as all-unsupported, so the
// status check fails closed before any write is attempted).
func (m ModeSupport) AdvanceToPending(mode TrustMode) ModeSupport {
	entry := m.Entry(mode)
	if entry.Status != ModeSupportRequested {
		return m
	}
	entry.Status = ModeSupportPending
	entry.Reason = "readiness evidence recorded, awaiting activation authority"
	return m.with(mode, entry)
}

// Suspend applies the `active -> suspended` transition from §7.2.0, whose
// sole authority is the readiness pipeline observing that evidence this
// activation depended on is no longer valid for the Capability's current
// version. A no-op for any other current status. reason is stored as
// ModeSupportEntry.Reason for operator visibility.
func (m ModeSupport) Suspend(mode TrustMode, reason string) ModeSupport {
	entry := m.Entry(mode)
	if entry.Status != ModeSupportActive {
		return m
	}
	entry.Status = ModeSupportSuspended
	entry.Reason = reason
	return m.with(mode, entry)
}

// Activate applies the `pending -> active` or `suspended -> active`
// transitions from §7.2.0, whose sole authority is an ActivationAuthority
// grant (see ActivationAuthority.Evaluate). A no-op for any other current
// status -- callers MUST NOT invoke this without a prior granted=true
// result; it performs no evaluation of its own.
func (m ModeSupport) Activate(mode TrustMode) ModeSupport {
	entry := m.Entry(mode)
	if entry.Status != ModeSupportPending && entry.Status != ModeSupportSuspended {
		return m
	}
	entry.Status = ModeSupportActive
	entry.Reason = ""
	return m.with(mode, entry)
}

type ProofRequirements struct {
	NetworkVerifiableReceipt bool `json:"network_verifiable_receipt,omitempty"`
	TOSSettlement            bool `json:"tos_settlement,omitempty"`
	PortableProofOfService   bool `json:"portable_proof_of_service,omitempty"`
}

func (r ProofRequirements) RequiresTOS() bool {
	return r.NetworkVerifiableReceipt || r.TOSSettlement || r.PortableProofOfService
}

type ProofDescriptor struct {
	QuoteCommitment  bool `json:"quote_commitment"`
	ExecutionReceipt bool `json:"execution_receipt"`
	SettlementProof  bool `json:"settlement_proof"`
	ProofOfService   bool `json:"proof_of_service"`
}

type FundingModel string

const (
	FundingManagedBalance       FundingModel = "managed_balance"
	FundingClientTOS            FundingModel = "client_tos"
	FundingClientSupportedAsset FundingModel = "client_supported_asset"
	FundingGatewaySponsored     FundingModel = "gateway_sponsored"
	FundingExternalSponsor      FundingModel = "external_sponsor"
)

type SettlementBackend string

const (
	SettlementATOSManaged     SettlementBackend = "atos_managed"
	SettlementTOS             SettlementBackend = "tos"
	SettlementExternalManaged SettlementBackend = "external_managed"
)

type SettlementDescriptor struct {
	Backend       SettlementBackend `json:"backend"`
	Escrow        bool              `json:"escrow"`
	FundingModel  FundingModel      `json:"funding_model,omitempty"`
	ClientAsset   string            `json:"client_asset,omitempty"`
	ProviderAsset string            `json:"provider_asset,omitempty"`
}

type ProofCheckpoint string

const (
	ProofNotRequired ProofCheckpoint = "not_required"
	ProofPending     ProofCheckpoint = "pending"
	ProofCommitted   ProofCheckpoint = "committed"
	ProofReserved    ProofCheckpoint = "reserved"
	ProofSigned      ProofCheckpoint = "signed"
	ProofVerified    ProofCheckpoint = "verified"
	ProofSettled     ProofCheckpoint = "settled"
	ProofReleased    ProofCheckpoint = "released"
	ProofDisputed    ProofCheckpoint = "disputed"
	ProofFailed      ProofCheckpoint = "failed"
)

type ProofStatus struct {
	Quote      ProofCheckpoint `json:"quote"`
	Escrow     ProofCheckpoint `json:"escrow"`
	Receipt    ProofCheckpoint `json:"receipt"`
	Settlement ProofCheckpoint `json:"settlement"`
}

func InitialProofStatus(mode TrustMode) ProofStatus {
	if mode == TrustModeManaged {
		return ProofStatus{
			Quote: ProofNotRequired, Escrow: ProofReserved,
			Receipt: ProofPending, Settlement: ProofPending,
		}
	}
	return ProofStatus{
		Quote: ProofCommitted, Escrow: ProofReserved,
		Receipt: ProofPending, Settlement: ProofPending,
	}
}

type TrustPolicy struct {
	DefaultRequestedTrustMode RequestedTrustMode `json:"default_requested_trust_mode"`
	MinimumForHighValue       TrustMode          `json:"minimum_for_high_value,omitempty"`
}

// ResolveTrustMode applies the v0.2 selection contract. Concrete requests
// either resolve exactly or fail. Auto chooses the least decentralized active
// mode that satisfies the caller's proof requirements, keeping mainstream UX
// inexpensive while still allowing requirements to force a stronger mode.
func ResolveTrustMode(requested RequestedTrustMode, requirements ProofRequirements, support ModeSupport) (TrustMode, ProofProfile, error) {
	if requested == "" {
		requested = RequestedTrustAuto
	}
	if !requested.Valid() {
		return "", "", fmt.Errorf("invalid requested_trust_mode %q", requested)
	}
	if concrete, ok := requested.Concrete(); ok {
		if !support.Active(concrete) {
			return "", "", fmt.Errorf("trust mode %q is not active", concrete)
		}
		if requirements.RequiresTOS() && concrete == TrustModeManaged {
			return "", "", fmt.Errorf("managed mode cannot satisfy requested TOS proof requirements")
		}
		return concrete, support.Entry(concrete).ProofProfile, nil
	}

	if !requirements.RequiresTOS() && support.Active(TrustModeManaged) {
		return TrustModeManaged, ProofProfileNone, nil
	}
	if support.Active(TrustModeVerified) {
		return TrustModeVerified, support.Entry(TrustModeVerified).ProofProfile, nil
	}
	if support.Active(TrustModeNative) {
		return TrustModeNative, support.Entry(TrustModeNative).ProofProfile, nil
	}
	return "", "", fmt.Errorf("no active trust mode satisfies the request")
}
