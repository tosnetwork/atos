package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

type CapabilityService struct {
	store store.Store
}

func NewCapabilityService(s store.Store) *CapabilityService {
	return &CapabilityService{store: s}
}

func (s *CapabilityService) Get(ctx context.Context, id string) (domain.Capability, error) {
	c, err := s.store.Get(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Capability{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
		}
		return domain.Capability{}, err
	}
	return normalizeCapability(c), nil
}

type RegisterCapabilityInput struct {
	ProviderID          string
	Name                string
	Description         string
	DeliveryMode        domain.DeliveryMode
	InputSchema         map[string]any
	OutputSchema        map[string]any
	Pricing             domain.Pricing
	Tags                []string
	RequestedTrustModes []domain.TrustMode
	Bindings            []domain.CapabilityBinding
	IdempotencyKey      string
}

func (s *CapabilityService) Register(ctx context.Context, in RegisterCapabilityInput) (domain.Capability, error) {
	if in.ProviderID == "" || in.Name == "" || in.Description == "" {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "provider_id, name and description are required", false)
	}
	if in.IdempotencyKey == "" {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	if in.DeliveryMode != domain.DeliveryInstant && in.DeliveryMode != domain.DeliveryAsync && in.DeliveryMode != domain.DeliveryInteractive {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "delivery_mode must be instant, async or interactive", false)
	}
	if in.InputSchema == nil || in.OutputSchema == nil {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "input_schema and output_schema are required", false)
	}
	if err := validateCapabilitySchemas(in.InputSchema, in.OutputSchema); err != nil {
		return domain.Capability{}, err
	}
	if err := validatePricing(in.Pricing); err != nil {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "invalid pricing: "+err.Error(), false)
	}
	requested, err := normalizeRequestedModes(in.RequestedTrustModes)
	if err != nil {
		return domain.Capability{}, err
	}
	bindings, err := normalizeBindings(in.Bindings, requested)
	if err != nil {
		return domain.Capability{}, err
	}

	requestHash := hashRequest(
		"register-capability", in.ProviderID, in.Name, in.Description,
		in.DeliveryMode, in.InputSchema, in.OutputSchema, in.Pricing,
		in.Tags, requested, bindings,
	)
	record, reserved, err := s.store.Reserve(ctx, in.ProviderID, in.IdempotencyKey, requestHash, time.Now().UTC().Add(idempotencyLease))
	if err != nil {
		return domain.Capability{}, err
	}
	if !reserved {
		if record.RequestHash != requestHash {
			return domain.Capability{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different registration", false)
		}
		if record.Status != store.IdempotencyCompleted {
			return domain.Capability{}, domain.NewError(domain.ErrIdempotencyConflict, "capability registration is still in progress", true)
		}
		return s.Get(ctx, record.ResponseKey)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(ctx, in.ProviderID, in.IdempotencyKey)
		}
	}()

	inputArtifacts := artifactFields(in.InputSchema)
	outputArtifacts := artifactFields(in.OutputSchema)
	now := time.Now().UTC()
	id := "cap_" + uuid.NewString()
	c := domain.Capability{
		ID:                       id,
		CanonicalURI:             "atos://capability/" + id,
		ProviderID:               in.ProviderID,
		Name:                     in.Name,
		Description:              in.Description,
		Version:                  "1.0.0",
		Tags:                     append([]string(nil), in.Tags...),
		DeliveryMode:             in.DeliveryMode,
		InputSchema:              in.InputSchema,
		OutputSchema:             in.OutputSchema,
		Pricing:                  in.Pricing,
		AdapterType:              bindings[0].Transport,
		Trust:                    domain.Trust{Score: 0, Level: domain.TrustSelfAsserted},
		RequestedTrustModes:      requested,
		ModeSupport:              initialModeSupport(requested),
		Bindings:                 bindings,
		RequiresArtifactTransfer: len(inputArtifacts)+len(outputArtifacts) > 0,
		ArtifactInputFields:      inputArtifacts,
		ArtifactOutputFields:     outputArtifacts,
		Status:                   domain.CapabilityActive,
		UpdatedAt:                now,
	}
	c.SupportedTrustModes = c.ModeSupport.ActiveModes()
	c.ManifestCommitment = capabilityManifestCommitment(c)
	if err := s.store.Put(ctx, c); err != nil {
		return domain.Capability{}, err
	}
	if err := s.store.Finish(ctx, in.ProviderID, in.IdempotencyKey, c.ID); err != nil {
		return domain.Capability{}, err
	}
	committed = true
	return c, nil
}

func (s *CapabilityService) Update(ctx context.Context, id, requestingProviderID string, patch map[string]any, idempotencyKey string) (domain.Capability, error) {
	if idempotencyKey == "" {
		return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "idempotency_key is required", false)
	}
	requestHash := hashRequest("update-capability", id, patch)
	record, reserved, err := s.store.Reserve(ctx, requestingProviderID, idempotencyKey, requestHash, time.Now().UTC().Add(idempotencyLease))
	if err != nil {
		return domain.Capability{}, err
	}
	if !reserved {
		if record.RequestHash != requestHash {
			return domain.Capability{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different capability update", false)
		}
		if record.Status != store.IdempotencyCompleted {
			return domain.Capability{}, domain.NewError(domain.ErrIdempotencyConflict, "capability update is still in progress", true)
		}
		return s.Get(ctx, record.ResponseKey)
	}
	committed := false
	defer func() {
		if !committed {
			_ = s.store.Release(ctx, requestingProviderID, idempotencyKey)
		}
	}()

	c, err := s.Get(ctx, id)
	if err != nil {
		return domain.Capability{}, err
	}
	if c.ProviderID != requestingProviderID {
		return domain.Capability{}, domain.NewError(domain.ErrPermissionDenied, "not the owning provider", false)
	}
	for _, immutable := range []string{"provider_id", "supported_trust_modes", "mode_support", "manifest_commitment", "canonical_uri"} {
		if _, attempted := patch[immutable]; attempted {
			return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, immutable+" cannot be set through a generic update", false)
		}
	}

	if name, ok := patch["name"].(string); ok && name != "" {
		c.Name = name
	}
	if desc, ok := patch["description"].(string); ok && desc != "" {
		c.Description = desc
	}
	if status, ok := patch["status"].(string); ok {
		switch domain.CapabilityStatus(status) {
		case domain.CapabilityActive, domain.CapabilityPaused:
			c.Status = domain.CapabilityStatus(status)
		default:
			return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, fmt.Sprintf("invalid status %q", status), false)
		}
	}

	termsChanged := false
	if pricing, ok := patch["pricing"]; ok {
		var p domain.Pricing
		b, _ := json.Marshal(pricing)
		if err := json.Unmarshal(b, &p); err != nil {
			return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "invalid pricing", false)
		}
		if err := validatePricing(p); err != nil {
			return domain.Capability{}, domain.NewError(domain.ErrValidationFailed, "invalid pricing: "+err.Error(), false)
		}
		if !samePricing(c.Pricing, p) {
			c.Pricing = p
			termsChanged = true
		}
	}
	if inputSchema, ok := patch["input_schema"].(map[string]any); ok {
		c.InputSchema = inputSchema
		termsChanged = true
	}
	if outputSchema, ok := patch["output_schema"].(map[string]any); ok {
		c.OutputSchema = outputSchema
		termsChanged = true
	}
	if raw, ok := patch["requested_trust_modes"]; ok {
		modes, err := decodeTrustModes(raw)
		if err != nil {
			return domain.Capability{}, err
		}
		c.RequestedTrustModes = modes
		c.ModeSupport = reconcileModeSupport(c.ModeSupport, modes)
	}
	if raw, ok := patch["bindings"]; ok {
		bindings, err := decodeBindings(raw, c.RequestedTrustModes)
		if err != nil {
			return domain.Capability{}, err
		}
		c.Bindings = bindings
		c.AdapterType = bindings[0].Transport
		termsChanged = true
	}
	if termsChanged {
		c.Version = bumpMinorVersion(c.Version)
	}

	// Validated against the fully-built candidate, after every patched
	// field has been applied but before anything is persisted -- a
	// schema-invalid patch must leave the stored Capability's version,
	// manifest commitment, schemas, and every other field byte-for-byte
	// unchanged, never a partial update.
	if err := validateCapabilitySchemas(c.InputSchema, c.OutputSchema); err != nil {
		return domain.Capability{}, err
	}

	c.ArtifactInputFields = artifactFields(c.InputSchema)
	c.ArtifactOutputFields = artifactFields(c.OutputSchema)
	c.RequiresArtifactTransfer = len(c.ArtifactInputFields)+len(c.ArtifactOutputFields) > 0
	c.SupportedTrustModes = c.ModeSupport.ActiveModes()
	c.UpdatedAt = time.Now().UTC()
	c.ManifestCommitment = capabilityManifestCommitment(c)

	if err := s.store.Put(ctx, c); err != nil {
		return domain.Capability{}, err
	}
	if err := s.store.Finish(ctx, requestingProviderID, idempotencyKey, c.ID); err != nil {
		return domain.Capability{}, err
	}
	committed = true
	return c, nil
}

// RecordReadinessEvidence applies the §7.2.0 `requested -> pending`
// transition for every trust mode eligible on transport's binding of
// capabilityID, triggered by the readiness pipeline (HealthService or
// CertificationService) recording a first evidence cycle for the
// Capability's CURRENT version. It is a no-op for a mode that has already
// moved past `requested`, so callers may invoke it unconditionally on
// every health check or certification attempt without checking prior
// state themselves.
func (s *CapabilityService) RecordReadinessEvidence(ctx context.Context, capabilityID string, transport domain.EndpointAdapterType) error {
	cap, err := s.Get(ctx, capabilityID)
	if err != nil {
		return err
	}
	changed := false
	for _, binding := range cap.Bindings {
		if binding.Transport != transport {
			continue
		}
		for _, mode := range binding.EligibleTrustModes {
			before := cap.ModeSupport.Entry(mode).Status
			cap.ModeSupport = cap.ModeSupport.AdvanceToPending(mode)
			if cap.ModeSupport.Entry(mode).Status != before {
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	cap.SupportedTrustModes = cap.ModeSupport.ActiveModes()
	cap.UpdatedAt = time.Now().UTC()
	return s.store.Put(ctx, cap)
}

// SuspendModeIfActive applies the §7.2.0 `active -> suspended` transition
// for every trust mode eligible on transport's binding of capabilityID,
// triggered by the readiness pipeline observing that evidence this
// activation depended on is no longer valid for the Capability's CURRENT
// version. reason is stored as the mode's ModeSupportEntry.Reason. A no-op
// for a mode that isn't currently active.
func (s *CapabilityService) SuspendModeIfActive(ctx context.Context, capabilityID string, transport domain.EndpointAdapterType, reason string) error {
	cap, err := s.Get(ctx, capabilityID)
	if err != nil {
		return err
	}
	changed := false
	for _, binding := range cap.Bindings {
		if binding.Transport != transport {
			continue
		}
		for _, mode := range binding.EligibleTrustModes {
			before := cap.ModeSupport.Entry(mode).Status
			cap.ModeSupport = cap.ModeSupport.Suspend(mode, reason)
			if cap.ModeSupport.Entry(mode).Status != before {
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	cap.SupportedTrustModes = cap.ModeSupport.ActiveModes()
	cap.UpdatedAt = time.Now().UTC()
	return s.store.Put(ctx, cap)
}

// EvaluateActivation is the activation authority's sole entry point for
// the §7.2.0 `pending -> active` and `suspended -> active` transitions.
// mode must currently be pending or suspended -- any other current status
// returns domain.ErrValidationFailed without calling authority at all,
// since those are the only two legal source states. A granted=false
// result from authority is not an error: it records reasonCode on the
// mode's entry for operator visibility and leaves Status unchanged.
func (s *CapabilityService) EvaluateActivation(ctx context.Context, authority domain.ActivationAuthority, capabilityID string, mode domain.TrustMode) (granted bool, reasonCode string, err error) {
	cap, err := s.Get(ctx, capabilityID)
	if err != nil {
		return false, "", err
	}
	status := cap.ModeSupport.Entry(mode).Status
	if status != domain.ModeSupportPending && status != domain.ModeSupportSuspended {
		return false, "", domain.NewError(domain.ErrValidationFailed, fmt.Sprintf("mode %q is %q, not pending or suspended", mode, status), false)
	}
	granted, reasonCode, err = authority.Evaluate(ctx, cap.ProviderID, cap.ID, cap.Version, mode)
	if err != nil {
		return false, "", err
	}
	if !granted {
		entry := cap.ModeSupport.Entry(mode)
		entry.Reason = reasonCode
		cap.ModeSupport[mode] = entry
		if err := s.store.Put(ctx, cap); err != nil {
			return false, "", err
		}
		return false, reasonCode, nil
	}
	cap.ModeSupport = cap.ModeSupport.Activate(mode)
	cap.SupportedTrustModes = cap.ModeSupport.ActiveModes()
	cap.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(ctx, cap); err != nil {
		return false, "", err
	}
	return true, reasonCode, nil
}

func (s *CapabilityService) ListByProvider(ctx context.Context, providerID string) ([]domain.Capability, error) {
	caps, err := s.store.ByProvider(ctx, providerID)
	if err != nil {
		return nil, err
	}
	for i := range caps {
		caps[i] = normalizeCapability(caps[i])
	}
	return caps, nil
}

func (s *CapabilityService) Pause(ctx context.Context, id, requestingProviderID, idempotencyKey string) (domain.Capability, error) {
	return s.Update(ctx, id, requestingProviderID, map[string]any{"status": string(domain.CapabilityPaused)}, idempotencyKey)
}

func (s *CapabilityService) Resume(ctx context.Context, id, requestingProviderID, idempotencyKey string) (domain.Capability, error) {
	return s.Update(ctx, id, requestingProviderID, map[string]any{"status": string(domain.CapabilityActive)}, idempotencyKey)
}

func (s *CapabilityService) Taxonomy(ctx context.Context) ([]string, error) {
	caps, err := s.store.Search(ctx, "", 1000)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var tags []string
	for _, c := range caps {
		for _, t := range c.Tags {
			if !seen[t] {
				seen[t] = true
				tags = append(tags, t)
			}
		}
	}
	sort.Strings(tags)
	return tags, nil
}

func normalizeRequestedModes(in []domain.TrustMode) ([]domain.TrustMode, error) {
	if len(in) == 0 {
		return []domain.TrustMode{domain.TrustModeManaged}, nil
	}
	seen := make(map[domain.TrustMode]bool)
	out := make([]domain.TrustMode, 0, len(in))
	for _, mode := range in {
		if !mode.Valid() {
			return nil, domain.NewError(domain.ErrValidationFailed, fmt.Sprintf("invalid requested trust mode %q", mode), false)
		}
		if !seen[mode] {
			seen[mode] = true
			out = append(out, mode)
		}
	}
	sort.Slice(out, func(i, j int) bool { return trustModeOrder(out[i]) < trustModeOrder(out[j]) })
	return out, nil
}

func trustModeOrder(mode domain.TrustMode) int {
	switch mode {
	case domain.TrustModeManaged:
		return 0
	case domain.TrustModeVerified:
		return 1
	case domain.TrustModeNative:
		return 2
	default:
		return 99
	}
}

// initialModeSupport applies atos-spec docs/IMPLEMENTATION_ROADMAP.md
// §7.2.0's frozen transition matrix at Capability registration:
// unsupported -> requested is the Provider's own authority (this function
// is only ever invoked as a direct consequence of the provider's own
// RequestedTrustModes), never unsupported -> pending or -> active for a
// stronger mode -- those require the readiness pipeline and the
// activation authority respectively, neither of which has run yet for a
// Capability that doesn't exist until this call returns. Managed is the
// sole exception: it has no readiness/activation concept and is
// unconditionally active the moment it's requested.
func initialModeSupport(requested []domain.TrustMode) domain.ModeSupport {
	out := domain.ModeSupport{
		domain.TrustModeManaged:  {Status: domain.ModeSupportUnsupported},
		domain.TrustModeVerified: {Status: domain.ModeSupportUnsupported, ProofProfile: domain.ProofProfileTOSVerifiedV1},
		domain.TrustModeNative:   {Status: domain.ModeSupportUnsupported, ProofProfile: domain.ProofProfileTOSNativeV1},
	}
	for _, mode := range requested {
		entry := out[mode]
		if mode == domain.TrustModeManaged {
			entry.Status = domain.ModeSupportActive
		} else {
			entry.Status = domain.ModeSupportRequested
			entry.Reason = "no readiness evidence recorded yet"
		}
		out[mode] = entry
	}
	return out
}

// reconcileModeSupport applies the same §7.2.0 transition matrix on a
// Capability update: only the Provider-authority edges
// (unsupported<->requested for a stronger mode, and any status ->
// unsupported when the provider stops requesting a mode) belong here.
// requested -> pending (readiness pipeline), pending/suspended -> active
// (activation authority), and active -> suspended (readiness pipeline)
// are driven elsewhere by whichever service actually observed the
// triggering event -- this function must never perform them itself,
// since a generic metadata PATCH is not evidence of any of those.
func reconcileModeSupport(current domain.ModeSupport, requested []domain.TrustMode) domain.ModeSupport {
	if current == nil {
		return initialModeSupport(requested)
	}
	requestedSet := make(map[domain.TrustMode]bool, len(requested))
	for _, mode := range requested {
		requestedSet[mode] = true
	}
	for _, mode := range []domain.TrustMode{domain.TrustModeManaged, domain.TrustModeVerified, domain.TrustModeNative} {
		entry := current.Entry(mode)
		if !requestedSet[mode] {
			entry.Status = domain.ModeSupportUnsupported
			entry.Reason = "not requested by provider"
		} else if entry.Status == domain.ModeSupportUnsupported {
			if mode == domain.TrustModeManaged {
				entry.Status = domain.ModeSupportActive
				entry.Reason = ""
			} else {
				entry.Status = domain.ModeSupportRequested
				entry.ProofProfile = domain.StandardProofProfile(mode)
				entry.Reason = "no readiness evidence recorded yet"
			}
		}
		current[mode] = entry
	}
	return current
}

func normalizeBindings(in []domain.CapabilityBinding, requested []domain.TrustMode) ([]domain.CapabilityBinding, error) {
	if len(in) == 0 {
		return []domain.CapabilityBinding{{
			Transport: domain.AdapterTOSNative, EndpointRef: "internal:mock",
			EligibleTrustModes: append([]domain.TrustMode(nil), requested...),
		}}, nil
	}
	out := make([]domain.CapabilityBinding, len(in))
	copy(out, in)
	for i := range out {
		if out[i].EndpointRef == "" {
			return nil, domain.NewError(domain.ErrValidationFailed, "binding endpoint_ref is required", false)
		}
		switch out[i].Transport {
		case domain.AdapterHTTP, domain.AdapterMCP, domain.AdapterA2A, domain.AdapterHuman, domain.AdapterTOSNative:
		default:
			return nil, domain.NewError(domain.ErrValidationFailed, "invalid binding transport", false)
		}
		modes, err := normalizeRequestedModes(out[i].EligibleTrustModes)
		if err != nil {
			return nil, err
		}
		out[i].EligibleTrustModes = modes
	}
	return out, nil
}

func decodeTrustModes(raw any) ([]domain.TrustMode, error) {
	b, _ := json.Marshal(raw)
	var modes []domain.TrustMode
	if err := json.Unmarshal(b, &modes); err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "requested_trust_modes must be an array of concrete modes", false)
	}
	return normalizeRequestedModes(modes)
}

func decodeBindings(raw any, requested []domain.TrustMode) ([]domain.CapabilityBinding, error) {
	b, _ := json.Marshal(raw)
	var bindings []domain.CapabilityBinding
	if err := json.Unmarshal(b, &bindings); err != nil {
		return nil, domain.NewError(domain.ErrValidationFailed, "invalid bindings", false)
	}
	return normalizeBindings(bindings, requested)
}

func normalizeCapability(c domain.Capability) domain.Capability {
	if len(c.RequestedTrustModes) == 0 {
		c.RequestedTrustModes = []domain.TrustMode{domain.TrustModeManaged}
	}
	if c.ModeSupport == nil {
		c.ModeSupport = initialModeSupport(c.RequestedTrustModes)
	}
	c.SupportedTrustModes = c.ModeSupport.ActiveModes()
	if c.CanonicalURI == "" && c.ID != "" {
		c.CanonicalURI = "atos://capability/" + c.ID
	}
	if c.ManifestCommitment == "" {
		c.ManifestCommitment = capabilityManifestCommitment(c)
	}
	if len(c.Bindings) == 0 {
		transport := c.AdapterType
		if transport == "" {
			transport = domain.AdapterTOSNative
		}
		c.Bindings = []domain.CapabilityBinding{{Transport: transport, EndpointRef: "internal:legacy", EligibleTrustModes: c.SupportedTrustModes}}
	}
	c.ArtifactInputFields = artifactFields(c.InputSchema)
	c.ArtifactOutputFields = artifactFields(c.OutputSchema)
	c.RequiresArtifactTransfer = len(c.ArtifactInputFields)+len(c.ArtifactOutputFields) > 0
	return c
}

func capabilityManifestCommitment(c domain.Capability) string {
	payload := struct {
		ID           string                     `json:"id"`
		ProviderID   string                     `json:"provider_id"`
		Version      string                     `json:"version"`
		InputSchema  map[string]any             `json:"input_schema"`
		OutputSchema map[string]any             `json:"output_schema"`
		Pricing      domain.Pricing             `json:"pricing"`
		DeliveryMode domain.DeliveryMode        `json:"delivery_mode"`
		Bindings     []domain.CapabilityBinding `json:"bindings"`
	}{c.ID, c.ProviderID, c.Version, c.InputSchema, c.OutputSchema, c.Pricing, c.DeliveryMode, c.Bindings}
	b, _ := json.Marshal(payload)
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

func artifactFields(schema map[string]any) []string {
	var out []string
	walkArtifactSchema(schema, "", &out)
	sort.Strings(out)
	return out
}

func walkArtifactSchema(node any, path string, out *[]string) {
	obj, ok := node.(map[string]any)
	if !ok {
		return
	}
	if isArtifactSchema(obj) && path != "" {
		*out = append(*out, path)
		return
	}
	if properties, ok := obj["properties"].(map[string]any); ok {
		for name, child := range properties {
			childPath := name
			if path != "" {
				childPath = path + "." + name
			}
			walkArtifactSchema(child, childPath, out)
		}
	}
	if items, ok := obj["items"]; ok {
		walkArtifactSchema(items, path+"[]", out)
	}
	for _, keyword := range []string{"oneOf", "anyOf", "allOf"} {
		if variants, ok := obj[keyword].([]any); ok {
			for _, variant := range variants {
				walkArtifactSchema(variant, path, out)
			}
		}
	}
}

func isArtifactSchema(obj map[string]any) bool {
	if marked, _ := obj["x-atos-artifact"].(bool); marked {
		return true
	}
	format, _ := obj["format"].(string)
	if format == "binary" || format == "byte" {
		return true
	}
	if ref, _ := obj["$ref"].(string); strings.Contains(strings.ToLower(ref), "artifact") {
		return true
	}
	if properties, ok := obj["properties"].(map[string]any); ok {
		if _, exists := properties["artifact_id"]; exists {
			return true
		}
	}
	return false
}

func samePricing(a, b domain.Pricing) bool {
	return a.Model == b.Model && a.Unit == b.Unit && a.PriceHint.Amount == b.PriceHint.Amount &&
		a.PriceHint.Currency == b.PriceHint.Currency && sameMeteredRates(a.MeteredRates, b.MeteredRates)
}

func sameMeteredRates(a, b *domain.MeteredRates) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func bumpMinorVersion(version string) string {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) != 3 {
		return "1.1.0"
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return "1.1.0"
	}
	return parts[0] + "." + strconv.Itoa(minor+1) + ".0"
}
