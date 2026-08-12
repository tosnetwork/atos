package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/tosnetwork/atos/internal/adapters/tosai"
	"github.com/tosnetwork/atos/internal/adapters/toscore"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/money"
	"github.com/tosnetwork/atos/internal/store"
	"github.com/tosnetwork/tos-protocol/pkg/quotecommitment"
)

const (
	quoteTTL            = 10 * time.Minute
	quoteDecimals       = 2
	verifiedTOSDecimals = 9
	defaultFeePerMille  = int64(50)
	defaultExecutionTTL = 15 * time.Minute
	defaultMaxOutput    = 1 << 20
)

type QuoteService struct {
	store            store.Store
	quoter           tosai.Quoter
	accounts         *AccountService
	core             toscore.Core
	signers          *ExecutionSignerService
	commitmentDomain string
}

func (s *QuoteService) WithVerifiedCommitmentAuthority(core toscore.Core, signers *ExecutionSignerService, domainName string) *QuoteService {
	s.core = core
	s.signers = signers
	s.commitmentDomain = strings.TrimSpace(domainName)
	if s.commitmentDomain == "" {
		s.commitmentDomain = "atos.im"
	}
	return s
}

// NewQuoteService accepts an optional provider/Edge quoter. Omitting it keeps
// the explicit Phase 0 static-price path; RPC deployments pass the real
// tos-protocol client and therefore obtain a two-layer Quote.
func NewQuoteService(s store.Store, quoters ...tosai.Quoter) *QuoteService {
	service := &QuoteService{store: s}
	if len(quoters) > 0 {
		service.quoter = quoters[0]
	}
	return service
}

// WithAccountService adds gateway spending-policy evaluation to Quote output.
// The execution path still rechecks policy before reserving funds, so this
// presentation hint can never bypass authorization or accounting.
func (s *QuoteService) WithAccountService(accounts *AccountService) *QuoteService {
	s.accounts = accounts
	return s
}

type CreateQuoteInput struct {
	PrincipalID        string
	CapabilityID       string
	InputSummary       map[string]any
	RequestedTrustMode domain.RequestedTrustMode
	ProofRequirements  domain.ProofRequirements
	MaxTotal           *domain.Money
	// IdempotencyKey is optional and backward-compatible: the many existing
	// callers (handleCreateQuote, toolQuote) that never set it keep the
	// original always-mint-a-fresh-Quote behavior unchanged. Phase 3C's
	// OpenTask acceptance flow is the first caller that sets it, since
	// acceptance must be able to retry/resume after a crash without minting
	// a second Quote for the same accepted proposal -- see Create's
	// Reserve/Finish/Release wrapper below, which mirrors JobService.submit.
	IdempotencyKey string
	// ExpectedCapabilityVersion is optional and backward-compatible (empty
	// skips the check, exactly like every pre-existing caller). When set,
	// Create refuses (domain.ErrQuoteMismatch, non-retryable) rather than
	// silently pricing against whatever the Capability's CURRENT version
	// happens to be if it no longer matches. This exists for callers that
	// pinned a Capability version at an earlier commitment point (Phase
	// 3C's OpenTaskProposal.CapabilityVersion, frozen at Propose time) and
	// must never let a version bump that happens between that commitment
	// and a later/resumed Quote-creation attempt (e.g. after a crash, or a
	// reconciler-driven retry) silently rebind to a different version.
	ExpectedCapabilityVersion string
}

// Create resolves trust mode, computes pricing and freezes a new Quote from
// the Capability's current state. When in.IdempotencyKey is empty this is
// exactly the original, always-fresh behavior. When it is set, Create
// becomes genuinely idempotent: the same (PrincipalID, IdempotencyKey) with
// the same business inputs always returns the same Quote (replay), the same
// key with different inputs is rejected as domain.ErrIdempotencyConflict,
// and a crash between committing the Quote row and marking the idempotency
// record Finished is recovered via QuoteByIdempotencyKey rather than
// minting a duplicate.
func (s *QuoteService) Create(ctx context.Context, in CreateQuoteInput) (domain.Quote, error) {
	if in.IdempotencyKey == "" {
		q, err := s.buildQuote(ctx, in)
		if err != nil {
			return domain.Quote{}, err
		}
		q, err = s.commitVerifiedQuote(ctx, q)
		if err != nil {
			return domain.Quote{}, err
		}
		if q.TrustMode == domain.TrustModeVerified {
			return s.projectVerifiedQuote(ctx, q)
		}
		if err := s.store.PutQuote(ctx, q); err != nil {
			return domain.Quote{}, err
		}
		return q, nil
	}
	if in.PrincipalID == "" {
		return domain.Quote{}, domain.NewError(domain.ErrAuthenticationRequired, "principal is required when idempotency_key is set", false)
	}

	requestHash := hashRequest("atos-quote-v1", in.CapabilityID, in.InputSummary,
		string(in.RequestedTrustMode), in.ProofRequirements, in.MaxTotal, in.ExpectedCapabilityVersion)
	now := time.Now().UTC().Truncate(time.Millisecond)
	rec, reserved, err := s.store.Reserve(ctx, in.PrincipalID, in.IdempotencyKey, requestHash, now.Add(idempotencyLease))
	if err != nil {
		return domain.Quote{}, err
	}
	if !reserved {
		if rec.RequestHash != requestHash {
			return domain.Quote{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		if rec.Status != store.IdempotencyCompleted {
			return s.awaitQuoteOperation(ctx, in.PrincipalID, in.IdempotencyKey, requestHash)
		}
		return s.store.GetQuote(ctx, rec.ResponseKey)
	}

	committed := false
	defer func() {
		if !committed {
			if _, lookupErr := s.store.QuoteCommitmentOperationByIdempotencyKey(context.Background(), in.PrincipalID, in.IdempotencyKey); lookupErr == store.ErrNotFound {
				_ = s.store.Release(context.Background(), in.PrincipalID, in.IdempotencyKey)
			}
		}
	}()
	if op, lookupErr := s.store.QuoteCommitmentOperationByIdempotencyKey(ctx, in.PrincipalID, in.IdempotencyKey); lookupErr == nil {
		if op.Quote.IdempotencyRequestHash != requestHash {
			return domain.Quote{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		q, resumeErr := s.resumeQuoteCommitmentOperation(ctx, op)
		if resumeErr == nil {
			committed = true
		}
		return q, resumeErr
	} else if lookupErr != store.ErrNotFound {
		return domain.Quote{}, lookupErr
	}

	// Recover a process crash after the Quote commit but before Finish. The
	// unique (principal_id,idempotency_key) index makes this unambiguous.
	//
	// The IdempotencyRequestHash comparison is required, not redundant with
	// the Reserve-time hash check above: if a PRIOR attempt's PutQuote
	// succeeded but that attempt's own Finish call then failed, its
	// deferred Release hard-deletes the idempotency_records row entirely
	// -- a LATER call reusing the same key, even with genuinely different
	// content, would see no existing reservation, reserve fresh, and
	// land here. Without comparing against what was actually committed,
	// it would silently receive the OLD Quote instead of being rejected
	// as a conflicting reuse of the key. See domain.Quote.IdempotencyRequestHash's
	// doc comment.
	if existing, lookupErr := s.store.QuoteByIdempotencyKey(ctx, in.PrincipalID, in.IdempotencyKey); lookupErr == nil {
		if existing.IdempotencyRequestHash != requestHash {
			return domain.Quote{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
		}
		if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, existing.ID); err != nil {
			return domain.Quote{}, err
		}
		committed = true
		return existing, nil
	} else if lookupErr != store.ErrNotFound {
		return domain.Quote{}, lookupErr
	}

	q, err := s.buildQuote(ctx, in)
	if err != nil {
		return domain.Quote{}, err
	}
	q.IdempotencyRequestHash = requestHash
	q.IdempotencyKey = in.IdempotencyKey
	q, err = s.commitVerifiedQuote(ctx, q)
	if err != nil {
		return domain.Quote{}, err
	}
	if q.TrustMode == domain.TrustModeVerified {
		q, err = s.projectVerifiedQuote(ctx, q)
		if err != nil {
			return domain.Quote{}, err
		}
	} else {
		if err := s.store.PutQuote(ctx, q); err != nil {
			return domain.Quote{}, err
		}
		if err := s.store.Finish(ctx, in.PrincipalID, in.IdempotencyKey, q.ID); err != nil {
			return domain.Quote{}, err
		}
	}
	committed = true
	return q, nil
}

// awaitQuoteOperation closes the small reservation-before-intent window seen
// by a competing replica. It never invents a second Quote: it waits briefly
// for the reservation owner to publish either the durable operation snapshot
// or the completed Quote, then resumes that exact identity.
func (s *QuoteService) awaitQuoteOperation(ctx context.Context, principalID, key, requestHash string) (domain.Quote, error) {
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if op, err := s.store.QuoteCommitmentOperationByIdempotencyKey(ctx, principalID, key); err == nil {
			if op.Quote.IdempotencyRequestHash != requestHash {
				return domain.Quote{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
			}
			return s.resumeQuoteCommitmentOperation(ctx, op)
		} else if err != store.ErrNotFound {
			return domain.Quote{}, err
		}
		if q, err := s.store.QuoteByIdempotencyKey(ctx, principalID, key); err == nil {
			if q.IdempotencyRequestHash != requestHash {
				return domain.Quote{}, domain.NewError(domain.ErrIdempotencyConflict, "idempotency_key reused with a different request", false)
			}
			return q, nil
		} else if err != store.ErrNotFound {
			return domain.Quote{}, err
		}
		select {
		case <-ctx.Done():
			return domain.Quote{}, ctx.Err()
		case <-timer.C:
			return domain.Quote{}, domain.NewError(domain.ErrIdempotencyConflict, "a request with this idempotency_key is still in progress; retry shortly", true)
		case <-ticker.C:
		}
	}
}

func (s *QuoteService) resumeQuoteCommitmentOperation(ctx context.Context, op domain.QuoteCommitmentOperation) (domain.Quote, error) {
	q, err := s.commitVerifiedQuote(ctx, op.Quote)
	if err != nil {
		return domain.Quote{}, err
	}
	return s.projectVerifiedQuote(ctx, q)
}

func (s *QuoteService) projectVerifiedQuote(ctx context.Context, q domain.Quote) (domain.Quote, error) {
	if err := s.store.PutQuote(ctx, q); err != nil {
		return domain.Quote{}, err
	}
	if q.IdempotencyKey != "" {
		if err := s.store.Finish(ctx, q.PrincipalID, q.IdempotencyKey, q.ID); err != nil {
			return domain.Quote{}, err
		}
	}
	completed := time.Now().UTC()
	op, err := s.store.UpdateQuoteCommitmentOperation(ctx, q.ID, func(current domain.QuoteCommitmentOperation) (domain.QuoteCommitmentOperation, error) {
		if current.Checkpoint == domain.QuoteCommitmentCompleted {
			return current, nil
		}
		current.Quote = q
		current.Checkpoint = domain.QuoteCommitmentCompleted
		current.FailureReason = ""
		current.UpdatedAt = completed
		current.CompletedAt = &completed
		return current, nil
	})
	if err != nil {
		return domain.Quote{}, err
	}
	return op.Quote, nil
}

func (s *QuoteService) ReconcileStaleOperations(ctx context.Context, cutoff time.Time, limit int) error {
	ops, err := s.store.StaleQuoteCommitmentOperations(ctx, cutoff, limit)
	if err != nil {
		return err
	}
	var joined error
	for _, op := range ops {
		if _, driveErr := s.resumeQuoteCommitmentOperation(ctx, op); driveErr != nil {
			joined = errors.Join(joined, driveErr)
		}
	}
	return joined
}
func (s *QuoteService) RunReconciler(ctx context.Context, interval, staleAfter time.Duration, limit int, report func(error)) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	if staleAfter <= 0 {
		staleAfter = 30 * time.Second
	}
	if limit <= 0 {
		limit = 100
	}
	sweep := func() {
		if err := s.ReconcileStaleOperations(ctx, time.Now().UTC().Add(-staleAfter), limit); err != nil && report != nil {
			report(err)
		}
	}
	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// buildQuote performs every validation/pricing/freezing step and returns a
// Quote ready to persist, without touching the store's idempotency records
// or calling PutQuote -- shared by both the plain and idempotent paths of
// Create above so the pricing/trust-mode/freezing algorithm exists in
// exactly one place.
func (s *QuoteService) buildQuote(ctx context.Context, in CreateQuoteInput) (domain.Quote, error) {
	if s.quoter != nil && in.PrincipalID == "" {
		return domain.Quote{}, domain.NewError(domain.ErrAuthenticationRequired, "principal is required for an RPC-backed Quote", false)
	}
	cap, err := s.store.Get(ctx, in.CapabilityID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability not found", false)
		}
		return domain.Quote{}, err
	}
	cap = normalizeCapability(cap)
	if cap.Status != domain.CapabilityActive {
		return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability is not active", false)
	}
	if in.ExpectedCapabilityVersion != "" && cap.Version != in.ExpectedCapabilityVersion {
		return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch,
			"capability version has changed since this request was committed", false)
	}

	requested := in.RequestedTrustMode
	if requested == "" {
		requested = domain.RequestedTrustAuto
	}
	mode, profile, resolveErr := domain.ResolveTrustMode(requested, in.ProofRequirements, cap.ModeSupport)
	if resolveErr != nil {
		if concrete, ok := requested.Concrete(); ok && !cap.Supports(concrete) {
			return domain.Quote{}, domain.NewError(domain.ErrTrustModeUnavailable, resolveErr.Error(), true)
		}
		return domain.Quote{}, domain.NewError(domain.ErrProofRequirementsUnsatisfied, resolveErr.Error(), true)
	}
	if mode != domain.TrustModeManaged && profile == domain.ProofProfileNone {
		return domain.Quote{}, domain.NewError(domain.ErrProofProfileUnavailable, "selected mode has no active proof profile", true)
	}
	if err := domain.ValidateCommittedTrust(mode, profile); err != nil {
		return domain.Quote{}, domain.NewError(domain.ErrProofProfileUnavailable, err.Error(), false)
	}

	priceDecimals := quoteDecimals
	if mode == domain.TrustModeVerified {
		if cap.Pricing.PriceHint.Currency != "TOS" {
			return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "verified capabilities must be priced directly in TOS", false)
		}
		priceDecimals = verifiedTOSDecimals
	}
	subtotal, err := money.Parse(cap.Pricing.PriceHint.Amount, cap.Pricing.PriceHint.Currency, priceDecimals)
	if err != nil {
		return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability has an invalid price_hint", false)
	}
	// Defense in depth: Register/Update already reject invalid MeteredRates,
	// but this catches a capability that was written before that validation
	// existed (or reached storage by some other path) before it gets frozen
	// into a new Quote and eventually causes an unrecoverable settlement
	// failure long after funds are committed.
	if err := validatePricing(cap.Pricing); err != nil {
		return domain.Quote{}, domain.NewError(domain.ErrCapabilityUnavailable, "capability has invalid metered pricing: "+err.Error(), false)
	}
	fees, err := applyFeeRate(subtotal, defaultFeePerMille)
	if err != nil {
		return domain.Quote{}, err
	}
	totalMax, err := subtotal.Add(fees)
	if err != nil {
		return domain.Quote{}, err
	}

	requiresConfirmation := false
	if s.accounts != nil && strings.TrimSpace(in.PrincipalID) != "" {
		requiresConfirmation, err = s.accounts.RequiresConfirmation(ctx, in.PrincipalID, totalMax.String(), totalMax.Currency)
		if err != nil {
			return domain.Quote{}, err
		}
	}

	if in.MaxTotal != nil {
		if in.MaxTotal.Currency == "" {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "constraints.max_total.currency is required", false)
		}
		if in.MaxTotal.Currency != subtotal.Currency {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed,
				fmt.Sprintf("constraints.max_total is in %s but this capability prices in %s", in.MaxTotal.Currency, subtotal.Currency), false)
		}
		bound, err := money.Parse(in.MaxTotal.Amount, subtotal.Currency, priceDecimals)
		if err != nil {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "invalid constraints.max_total.amount", false)
		}
		if totalMax.Cmp(bound) > 0 {
			return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "capability price exceeds requested max_total", false)
		}
	}

	settlement, proof := quoteGuarantees(mode, subtotal.Currency)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if mode == domain.TrustModeVerified {
		// TaskEscrow stores deadlines as integer seconds. Freeze Verified
		// Quote deadlines on that exact boundary before commitment so the
		// economic contract never shortens signed millisecond terms.
		now = now.Truncate(time.Second)
	}
	executionDeadline := now.Add(defaultExecutionTTL)
	if cap.SLA.TimeoutMS > 0 {
		executionDeadline = now.Add(time.Duration(cap.SLA.TimeoutMS) * time.Millisecond)
	}
	inputSummaryBytes, err := json.Marshal(in.InputSummary)
	if err != nil {
		return domain.Quote{}, domain.NewError(domain.ErrValidationFailed, "input_summary is not serializable", false)
	}
	inputSummaryCommitment := hashCommitment(in.InputSummary)
	var serviceQuote tosai.ServiceExecutionQuote
	if s.quoter != nil {
		serviceQuote, err = s.quoter.QuoteExecution(ctx, tosai.QuoteExecutionRequest{
			Capability: cap, InputSummary: in.InputSummary,
			InputCommitment: inputSummaryCommitment,
			InputBytes:      uint64(len(inputSummaryBytes)), MaxOutputBytes: defaultMaxOutput,
			ExecutionDeadline: executionDeadline,
			TrustMode:         mode, ProofProfile: profile,
		})
		if err != nil {
			return domain.Quote{}, err
		}
		if serviceQuote.ID == "" || serviceQuote.ExpiresAt.IsZero() || serviceQuote.ExecutionDeadline.IsZero() {
			return domain.Quote{}, domain.NewError(domain.ErrProviderFailed, "tos-protocol returned an incomplete service execution quote", true)
		}
		executionDeadline = serviceQuote.ExecutionDeadline.UTC().Truncate(time.Millisecond)
	}
	disputePolicyHash := termsHash("atos-dispute-policy", "v0.2", "72h")
	// Frozen here, at the same Capability snapshot ("cap") already used
	// above for pricing/trust-mode resolution -- not re-derived from a
	// possibly-later live Capability at Job-creation time. See
	// domain.Quote.Binding's doc comment: an already-issued Quote/Job MUST
	// continue to use its frozen version/binding semantics after a later
	// Capability update (atos-spec docs/IMPLEMENTATION_ROADMAP.md §7.1.0).
	// nil (not an error) when the Capability has no transport binding at
	// all -- the ordinary tos-native/human path.
	var frozenBinding *domain.CapabilityBinding
	if binding, ok := domain.SelectBinding(cap.Bindings, mode); ok {
		frozenBinding = &binding
	}
	q := domain.Quote{
		ID:                 "q_" + uuid.NewString(),
		CapabilityID:       cap.ID,
		CapabilityVersion:  cap.Version,
		ProviderID:         cap.ProviderID,
		PrincipalID:        in.PrincipalID,
		RequestedTrustMode: requested,
		TrustMode:          mode,
		ProofProfile:       profile,
		Price: domain.Price{
			Subtotal: subtotal.String(),
			Fees:     fees.String(),
			TotalMax: totalMax.String(),
			Currency: subtotal.Currency,
		},
		Settlement:             settlement,
		Proof:                  proof,
		ExpiresAt:              now.Add(quoteTTL),
		DisputePolicyHash:      disputePolicyHash,
		InputSummaryCommitment: inputSummaryCommitment,
		ExecutionDeadline:      executionDeadline,
		CreatedAt:              now,
		RequiresConfirmation:   requiresConfirmation,
		// Frozen here, not re-read at settlement time: metered billing must
		// never reinterpret an old Job using the Capability's current live
		// pricing configuration.
		MeteredRates: cap.Pricing.MeteredRates,
		PricingModel: cap.Pricing.Model,
		Binding:      frozenBinding,
		InputSchema:  cap.InputSchema,
		OutputSchema: cap.OutputSchema,
	}
	if serviceQuote.ID != "" {
		q.ServiceQuoteID = serviceQuote.ID
		q.UnderlyingServiceQuoteRef = serviceQuote.Reference
		if serviceQuote.ExpiresAt.Before(q.ExpiresAt) {
			q.ExpiresAt = serviceQuote.ExpiresAt.UTC().Truncate(time.Millisecond)
		}
	}
	if mode == domain.TrustModeVerified &&
		(q.ExpiresAt.UnixMilli()%1000 != 0 || q.ExecutionDeadline.UnixMilli()%1000 != 0) {
		return domain.Quote{}, domain.NewError(
			domain.ErrValidationFailed,
			"Verified Quote and execution deadlines must be exactly second-aligned",
			false,
		)
	}
	q.TermsHash = termsHash(
		q.CapabilityID, q.CapabilityVersion, q.ProviderID, q.PrincipalID,
		string(q.TrustMode), string(q.ProofProfile),
		// The full frozen pricing contract must be committed here, not just
		// TotalMax: two Quotes can share the same TotalMax while splitting
		// it differently between Subtotal/Fees, or while metering usage at
		// entirely different per-dimension rates, and both differences
		// change what a Job ultimately gets charged and what the provider
		// ultimately earns (internal/service/billing.go). PricingModel and
		// MeteredRates are therefore part of this commitment, not just
		// recorded for audit trail (see domain.Quote.PricingModel /
		// MeteredRates doc comments).
		string(q.PricingModel), q.Price.Subtotal, q.Price.Fees, q.Price.TotalMax, q.Price.Currency,
		hashCommitment(q.MeteredRates),
		string(q.Settlement.Backend), string(q.Settlement.FundingModel),
		q.ExpiresAt.Format(time.RFC3339Nano), q.ExecutionDeadline.Format(time.RFC3339Nano),
		q.DisputePolicyHash, q.ServiceQuoteID, q.UnderlyingServiceQuoteRef,
		q.InputSummaryCommitment,
		// Binding/InputSchema/OutputSchema are the exact execution semantics
		// Job creation later freezes from this Quote (see
		// domain.Quote.Binding's doc comment) -- committed here for the same
		// reason PricingModel/MeteredRates are: two Quotes must not share a
		// TermsHash while differing in what a Job would actually execute
		// against.
		hashCommitment(q.Binding), hashCommitment(q.InputSchema), hashCommitment(q.OutputSchema),
	)
	if q.TrustMode == domain.TrustModeVerified {
		if s.core == nil || s.signers == nil || s.core.Network() == "" {
			return domain.Quote{}, domain.NewError(domain.ErrNetworkUnavailable, "verified quote commitment authority is unavailable", true)
		}
		q.NetworkID, q.CommitmentDomain, q.AssetDecimals = s.core.Network(), s.commitmentDomain, uint32(priceDecimals)
		q.CommitmentVersion, q.CommitmentCanonicalization = quotecommitment.Version, quotecommitment.Canonicalization
		requester, bound, revoked, _, err := s.core.ResolvePrincipalBindingStatus(ctx, q.PrincipalID)
		if err != nil {
			return domain.Quote{}, err
		}
		if !bound || revoked || requester.Network != q.NetworkID {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "requester identity is not currently bound on the configured network", false)
		}
		q.RequesterAgentID = requester.AgentID
		provider, providerBound, providerRevoked, _, err := s.core.ResolvePrincipalBindingStatus(ctx, q.ProviderID)
		if err != nil {
			return domain.Quote{}, err
		}
		if !providerBound || providerRevoked || provider.Network != q.NetworkID {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "provider identity is not currently bound on the configured network", false)
		}
		if cap.ManifestCommitment == "" || cap.Ownership.Status != domain.OwnershipAnchored || cap.Ownership.Network != q.NetworkID || cap.Ownership.Commitment == "" {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "capability manifest/ownership is not currently anchored", false)
		}
		verified, _, err := s.core.VerifyCapabilityOwnership(ctx, q.CapabilityID, q.ProviderID, q.CapabilityVersion, cap.ManifestCommitment)
		if err != nil {
			return domain.Quote{}, err
		}
		if !verified {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "capability ownership is not current", false)
		}
		q.ManifestCommitment = cap.ManifestCommitment
		q.OwnershipRef = cap.Ownership.Commitment
		authID, signerID, found, err := s.signers.SignerAt(ctx, q.CapabilityID, q.CapabilityVersion)
		if err != nil {
			return domain.Quote{}, err
		}
		if !found {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "execution signer authorization is missing", false)
		}
		auth, valid, err := s.core.ResolveExecutionSignerAuthorization(ctx, q.ProviderID, q.CapabilityID, q.CapabilityVersion, signerID, time.Now().UTC())
		if err != nil {
			return domain.Quote{}, err
		}
		if !valid || auth.Revoked || auth.AuthorizationID != authID || auth.AuthorizationRef == "" {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "execution signer authorization is stale, revoked, or mismatched", false)
		}
		q.SignerAuthorizationID = auth.AuthorizationID
		q.SignerAuthorizationRef = auth.AuthorizationRef
		q.TermsHash = termsHash("atos-verified-quote-terms-v1", q.TermsHash, q.NetworkID, q.CommitmentDomain, q.RequesterAgentID, q.ManifestCommitment, q.OwnershipRef, q.SignerAuthorizationID, q.SignerAuthorizationRef, fmt.Sprint(q.AssetDecimals))
	}
	return q, nil
}

func (s *QuoteService) commitVerifiedQuote(ctx context.Context, q domain.Quote) (domain.Quote, error) {
	if q.TrustMode != domain.TrustModeVerified {
		return q, nil
	}
	contentHash := termsHash("atos-verified-quote-operation-v1", q.ID, q.TermsHash, q.NetworkID, q.CommitmentDomain, q.ManifestCommitment, q.OwnershipRef, q.SignerAuthorizationID, q.SignerAuthorizationRef)
	now := time.Now().UTC()
	op, created, err := s.store.OpenQuoteCommitment(ctx, domain.QuoteCommitmentOperation{QuoteID: q.ID, Quote: q, ContentHash: contentHash, Checkpoint: domain.QuoteCommitmentIntentPersisted, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		return domain.Quote{}, err
	}
	if !created {
		if q.IdempotencyKey != "" && op.Quote.PrincipalID == q.PrincipalID && op.Quote.IdempotencyKey == q.IdempotencyKey && op.Quote.IdempotencyRequestHash == q.IdempotencyRequestHash {
			q = op.Quote
			contentHash = op.ContentHash
		} else if op.ContentHash != contentHash {
			return domain.Quote{}, domain.NewError(domain.ErrIdempotencyConflict, "quote commitment identity has different semantics", false)
		}
	}
	q = op.Quote
	if op.Checkpoint == domain.QuoteCommitmentCompleted {
		return op.Quote, nil
	}
	commitment, found, resolveErr := s.core.GetQuoteCommitment(ctx, q)
	if resolveErr != nil {
		current, updateErr := s.store.UpdateQuoteCommitmentOperation(context.Background(), q.ID, func(current domain.QuoteCommitmentOperation) (domain.QuoteCommitmentOperation, error) {
			if current.Checkpoint == domain.QuoteCommitmentCompleted {
				return current, nil
			}
			current.Checkpoint = domain.QuoteCommitmentReconciling
			current.FailureReason = resolveErr.Error()
			current.UpdatedAt = time.Now().UTC()
			return current, nil
		})
		if updateErr == nil && current.Checkpoint == domain.QuoteCommitmentCompleted {
			return current.Quote, nil
		}
		return domain.Quote{}, resolveErr
	}
	if !found {
		commitment, err = s.core.CommitQuote(ctx, q)
		if err != nil {
			current, updateErr := s.store.UpdateQuoteCommitmentOperation(context.Background(), q.ID, func(current domain.QuoteCommitmentOperation) (domain.QuoteCommitmentOperation, error) {
				if current.Checkpoint == domain.QuoteCommitmentCompleted {
					return current, nil
				}
				current.Checkpoint = domain.QuoteCommitmentReconciling
				current.FailureReason = err.Error()
				current.UpdatedAt = time.Now().UTC()
				return current, nil
			})
			if updateErr == nil && current.Checkpoint == domain.QuoteCommitmentCompleted {
				return current.Quote, nil
			}
			return domain.Quote{}, err
		}
	}
	if err := validateQuoteCommitment(q, commitment); err != nil {
		return domain.Quote{}, err
	}
	q.Commitment = &domain.QuoteCommitmentProjection{State: "committed", Network: commitment.Network, Reference: commitment.Reference, Digest: commitment.Digest, Finalized: commitment.Finalized, FinalizedCheckpoint: commitment.FinalizedCheckpoint}
	committedAt := time.Now().UTC()
	op, err = s.store.UpdateQuoteCommitmentOperation(ctx, q.ID, func(current domain.QuoteCommitmentOperation) (domain.QuoteCommitmentOperation, error) {
		if current.ContentHash != contentHash {
			return current, domain.NewError(domain.ErrIdempotencyConflict, "quote commitment operation changed", false)
		}
		if current.Checkpoint == domain.QuoteCommitmentCompleted {
			return current, nil
		}
		current.Quote = q
		current.Checkpoint = domain.QuoteCommitmentAuthorityCommitted
		current.FailureReason = ""
		current.UpdatedAt = committedAt
		return current, nil
	})
	return op.Quote, err
}

func validateQuoteCommitment(q domain.Quote, c toscore.QuoteCommitment) error {
	if c.Quote.ID != q.ID || c.Quote.TermsHash != q.TermsHash || c.Quote.PrincipalID != q.PrincipalID || c.Quote.ProviderID != q.ProviderID || c.Quote.CapabilityID != q.CapabilityID || c.Quote.CapabilityVersion != q.CapabilityVersion || c.Quote.TrustMode != q.TrustMode || c.Quote.ProofProfile != q.ProofProfile || c.Quote.NetworkID != q.NetworkID || c.Quote.CommitmentDomain != q.CommitmentDomain || c.Quote.CommitmentVersion != quotecommitment.Version || c.Quote.CommitmentCanonicalization != quotecommitment.Canonicalization || c.Quote.RequesterAgentID != q.RequesterAgentID || c.Quote.ManifestCommitment != q.ManifestCommitment || c.Quote.OwnershipRef != q.OwnershipRef || c.Quote.SignerAuthorizationID != q.SignerAuthorizationID || c.Quote.SignerAuthorizationRef != q.SignerAuthorizationRef || c.Quote.Price.Subtotal != q.Price.Subtotal || c.Quote.Price.Fees != q.Price.Fees || c.Quote.Price.TotalMax != q.Price.TotalMax || c.Quote.Price.Currency != q.Price.Currency || c.Quote.AssetDecimals != q.AssetDecimals || !c.Quote.ExpiresAt.Equal(q.ExpiresAt) || !c.Quote.ExecutionDeadline.Equal(q.ExecutionDeadline) || c.Quote.DisputePolicyHash != q.DisputePolicyHash || c.Quote.ServiceQuoteID != q.ServiceQuoteID || c.Quote.Settlement.Backend != q.Settlement.Backend || c.Quote.Settlement.ProviderAsset != q.Settlement.ProviderAsset || c.Network != q.NetworkID || !c.Finalized || c.FinalizedCheckpoint == 0 || c.Reference == "" || c.Digest == "" || c.ExpectedDigest == "" || c.Digest != c.ExpectedDigest {
		return domain.NewError(domain.ErrQuoteMismatch, "canonical quote commitment is absent, non-final, or mismatched", false)
	}
	return nil
}

func quoteGuarantees(mode domain.TrustMode, clientAsset string) (domain.SettlementDescriptor, domain.ProofDescriptor) {
	if mode == domain.TrustModeManaged {
		return domain.SettlementDescriptor{
			Backend: domain.SettlementATOSManaged, Escrow: true,
			FundingModel: domain.FundingManagedBalance, ClientAsset: clientAsset,
		}, domain.ProofDescriptor{ExecutionReceipt: true}
	}
	return domain.SettlementDescriptor{
			Backend: domain.SettlementTOS, Escrow: true,
			FundingModel: domain.FundingGatewaySponsored,
			ClientAsset:  clientAsset, ProviderAsset: "TOS",
		}, domain.ProofDescriptor{
			QuoteCommitment: true, ExecutionReceipt: true,
			SettlementProof: true, ProofOfService: true,
		}
}

func (s *QuoteService) Get(ctx context.Context, id string) (domain.Quote, error) {
	q, err := s.store.GetQuote(ctx, id)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Quote{}, domain.NewError(domain.ErrNotFound, "quote not found", false)
		}
		return domain.Quote{}, err
	}
	if q.TrustMode == "" {
		q.RequestedTrustMode = domain.RequestedTrustManaged
		q.TrustMode = domain.TrustModeManaged
		q.Settlement, q.Proof = quoteGuarantees(q.TrustMode, q.Price.Currency)
	}
	if q.TrustMode == domain.TrustModeVerified {
		if s.core == nil || q.Commitment == nil || q.Commitment.State != "committed" {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "verified quote is not committed", false)
		}
		commitment, found, resolveErr := s.core.GetQuoteCommitment(ctx, q)
		if resolveErr != nil {
			return domain.Quote{}, resolveErr
		}
		if !found {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "verified quote commitment is missing", false)
		}
		if err := validateQuoteCommitment(q, commitment); err != nil {
			return domain.Quote{}, err
		}
		if q.Commitment.Reference != commitment.Reference || q.Commitment.Digest != commitment.Digest {
			return domain.Quote{}, domain.NewError(domain.ErrQuoteMismatch, "verified quote commitment projection is mismatched", false)
		}
	}
	return q, nil
}

func applyFeeRate(amount money.Amount, ratePerMille int64) (money.Amount, error) {
	if ratePerMille < 0 {
		return money.Amount{}, domain.NewError(domain.ErrValidationFailed, "fee rate cannot be negative", false)
	}
	rate := big.NewInt(ratePerMille)
	scaled := new(big.Int).Mul(amount.Minor, rate)
	scaled.Div(scaled, big.NewInt(1000))
	return money.Amount{Minor: scaled, Currency: amount.Currency, Decimals: amount.Decimals}, nil
}

func termsHash(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return "sha256:" + hex.EncodeToString(h[:])
}
