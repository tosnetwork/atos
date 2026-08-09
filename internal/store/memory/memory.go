// Package memory is the Phase 0 in-memory implementation of store.Store —
// enough to run and test the full ATOS contract locally without standing up
// Postgres. It is not durable and not safe past a single process.
package memory

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

type Store struct {
	mu                   sync.Mutex
	capabilities         map[string]domain.Capability
	quotes               map[string]domain.Quote
	escrows              map[string]domain.Escrow
	receipts             map[string]domain.Receipt
	receiptsByJob        map[string]string // jobID -> receiptID
	jobs                 map[string]domain.Job
	streamEvents         map[string][]domain.JobEvent      // jobID -> ordered events
	streamEventHashes    map[string][]string               // jobID -> content hash of the pristine incoming event at that index, mirroring Postgres's content_hash column
	streamCursors        map[string]domain.JobStreamCursor // jobID -> cursor
	streamDigests        map[string]streamDigestState      // jobID -> resumable hasher state
	accounts             map[string]domain.Account
	billingSnapshots     map[string]domain.BillingSnapshot // jobID -> snapshot
	earnings             map[string]domain.ProviderEarning // earningID -> earning
	earningsBySettlement map[string]string                 // settlementID -> earningID
	artifacts            map[string]domain.StoredArtifact
	idempotency          map[string]store.IdempotencyRecord // principalID+":"+key
}

func New() *Store {
	return &Store{
		capabilities:         make(map[string]domain.Capability),
		quotes:               make(map[string]domain.Quote),
		escrows:              make(map[string]domain.Escrow),
		receipts:             make(map[string]domain.Receipt),
		receiptsByJob:        make(map[string]string),
		jobs:                 make(map[string]domain.Job),
		streamEvents:         make(map[string][]domain.JobEvent),
		streamEventHashes:    make(map[string][]string),
		streamCursors:        make(map[string]domain.JobStreamCursor),
		streamDigests:        make(map[string]streamDigestState),
		accounts:             make(map[string]domain.Account),
		billingSnapshots:     make(map[string]domain.BillingSnapshot),
		earnings:             make(map[string]domain.ProviderEarning),
		earningsBySettlement: make(map[string]string),
		artifacts:            make(map[string]domain.StoredArtifact),
		idempotency:          make(map[string]store.IdempotencyRecord),
	}
}

// A single coarse mutex guards every collection below. Phase 0 favors
// obvious correctness over fine-grained locking — a real Postgres
// implementation gets its atomicity from row/transaction semantics instead.

func (s *Store) Put(ctx context.Context, c domain.Capability) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capabilities[c.ID] = c
	return nil
}

func (s *Store) Get(ctx context.Context, id string) (domain.Capability, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.capabilities[id]
	if !ok {
		return domain.Capability{}, store.ErrNotFound
	}
	return c, nil
}

// Search does a naive substring match over name/description/tags. It is a
// Phase 0 stand-in for the semantic search described in
// docs/CAPABILITIES.md — good enough to exercise the full contract, not
// good enough for production ranking.
func (s *Store) Search(ctx context.Context, query string, limit int) ([]domain.Capability, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := strings.ToLower(strings.TrimSpace(query))
	var out []domain.Capability
	for _, c := range s.capabilities {
		if c.Status != domain.CapabilityActive {
			continue
		}
		if q == "" || strings.Contains(strings.ToLower(c.Name), q) ||
			strings.Contains(strings.ToLower(c.Description), q) ||
			containsTag(c.Tags, q) {
			out = append(out, c)
		}
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func containsTag(tags []string, q string) bool {
	for _, t := range tags {
		if strings.Contains(strings.ToLower(t), q) {
			return true
		}
	}
	return false
}

func (s *Store) ByProvider(ctx context.Context, providerID string) ([]domain.Capability, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Capability
	for _, c := range s.capabilities {
		if c.ProviderID == providerID {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *Store) PutQuote(ctx context.Context, q domain.Quote) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quotes[q.ID] = q
	return nil
}

func (s *Store) GetQuote(ctx context.Context, id string) (domain.Quote, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.quotes[id]
	if !ok {
		return domain.Quote{}, store.ErrNotFound
	}
	return q, nil
}

func (s *Store) PutEscrow(ctx context.Context, e domain.Escrow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.escrows[e.ID] = e
	return nil
}

func (s *Store) GetEscrow(ctx context.Context, id string) (domain.Escrow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.escrows[id]
	if !ok {
		return domain.Escrow{}, store.ErrNotFound
	}
	return e, nil
}

func (s *Store) EscrowByJob(ctx context.Context, jobID string) (domain.Escrow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.escrows {
		if e.JobID == jobID {
			return e, nil
		}
	}
	return domain.Escrow{}, store.ErrNotFound
}

func (s *Store) PutReceipt(ctx context.Context, r domain.Receipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receipts[r.ID] = r
	s.receiptsByJob[r.JobID] = r.ID
	return nil
}

func (s *Store) GetReceipt(ctx context.Context, id string) (domain.Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.receipts[id]
	if !ok {
		return domain.Receipt{}, store.ErrNotFound
	}
	return r, nil
}

func (s *Store) ReceiptByJob(ctx context.Context, jobID string) (domain.Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.receiptsByJob[jobID]
	if !ok {
		return domain.Receipt{}, store.ErrNotFound
	}
	return s.receipts[id], nil
}

func (s *Store) ReceiptsByPrincipal(ctx context.Context, principalID string) ([]domain.Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Receipt
	for _, r := range s.receipts {
		if r.PrincipalID == principalID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *Store) PutJob(ctx context.Context, j domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
	return nil
}

func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, store.ErrNotFound
	}
	return j, nil
}

func (s *Store) JobsByPrincipal(ctx context.Context, principalID string) ([]domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Job
	for _, j := range s.jobs {
		if j.PrincipalID == principalID {
			out = append(out, j)
		}
	}
	return out, nil
}

func (s *Store) JobsForRecovery(ctx context.Context, updatedBefore time.Time, limit int) ([]domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	out := make([]domain.Job, 0, limit)
	for _, j := range s.jobs {
		if j.State.Terminal() || j.State == domain.JobInputRequired {
			continue
		}
		if !j.UpdatedAt.IsZero() && j.UpdatedAt.After(updatedBefore) {
			continue
		}
		out = append(out, j)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) JobByConfirmationCode(ctx context.Context, userCode string) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.Confirmation != nil && j.Confirmation.UserCode == userCode {
			return j, nil
		}
	}
	return domain.Job{}, store.ErrNotFound
}

func (s *Store) JobByIdempotencyKey(ctx context.Context, principalID, key string) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.PrincipalID == principalID && j.IdempotencyKey == key {
			return j, nil
		}
	}
	return domain.Job{}, store.ErrNotFound
}

// UpdateJob holds the store lock for the whole read-modify-write so two
// callers (e.g. the execution pipeline and Cancel) can never both act on
// the same stale job state.
func (s *Store) UpdateJob(ctx context.Context, id string, fn func(j domain.Job, exists bool) (domain.Job, error)) (domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.jobs[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.Job{}, err
	}
	s.jobs[id] = next
	return next, nil
}

func (s *Store) GetAccount(ctx context.Context, principalID string) (domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[principalID]
	if !ok {
		return domain.Account{}, store.ErrNotFound
	}
	return a, nil
}

func (s *Store) PutAccount(ctx context.Context, a domain.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[a.PrincipalID] = a
	return nil
}

// UpdateAccount holds the store lock for the whole read-modify-write, so
// concurrent debits/credits against the same principal serialize instead
// of racing on a stale balance. If the account doesn't exist yet, seed is
// used as the starting state (with exists=false passed to fn) instead of
// the zero value, so callers don't need a separate seeding round trip.
func (s *Store) UpdateAccount(ctx context.Context, principalID string, seed domain.Account, fn func(a domain.Account, exists bool) (domain.Account, error)) (domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.accounts[principalID]
	if !exists {
		current = seed
	}
	next, err := fn(current, exists)
	if err != nil {
		return domain.Account{}, err
	}
	s.accounts[principalID] = next
	return next, nil
}

func (s *Store) UpdateJobAndAccount(
	ctx context.Context, jobID, principalID string, seed domain.Account,
	fn func(domain.Job, bool, domain.Account, bool) (domain.Job, domain.Account, error),
) (domain.Job, domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, jobExists := s.jobs[jobID]
	account, accountExists := s.accounts[principalID]
	if !accountExists {
		account = seed
	}
	nextJob, nextAccount, err := fn(job, jobExists, account, accountExists)
	if err != nil {
		return domain.Job{}, domain.Account{}, err
	}
	if nextJob.ID != jobID || nextAccount.PrincipalID != principalID {
		return domain.Job{}, domain.Account{}, store.ErrConflict
	}
	s.jobs[jobID] = nextJob
	s.accounts[principalID] = nextAccount
	return nextJob, nextAccount, nil
}

func (s *Store) PutArtifact(ctx context.Context, a domain.StoredArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.artifacts[a.ID] = a
	return nil
}

func (s *Store) GetArtifact(ctx context.Context, id string) (domain.StoredArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.artifacts[id]
	if !ok {
		return domain.StoredArtifact{}, store.ErrNotFound
	}
	return a, nil
}

func (s *Store) Reserve(ctx context.Context, principalID, key, requestHash string, leaseUntil time.Time) (store.IdempotencyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	compositeKey := principalID + ":" + key
	now := time.Now().UTC()
	if existing, ok := s.idempotency[compositeKey]; ok {
		if existing.Status == store.IdempotencyInProgress &&
			existing.RequestHash == requestHash &&
			!existing.LeaseExpires.IsZero() && !now.Before(existing.LeaseExpires) {
			existing.ReservedAt = now
			existing.LeaseExpires = leaseUntil.UTC()
			s.idempotency[compositeKey] = existing
			return existing, true, nil
		}
		return existing, false, nil
	}
	rec := store.IdempotencyRecord{
		RequestHash:  requestHash,
		Status:       store.IdempotencyInProgress,
		ReservedAt:   now,
		LeaseExpires: leaseUntil.UTC(),
	}
	s.idempotency[compositeKey] = rec
	return rec, true, nil
}

func (s *Store) Finish(ctx context.Context, principalID, key, responseKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	compositeKey := principalID + ":" + key
	rec, ok := s.idempotency[compositeKey]
	if !ok {
		return store.ErrNotFound
	}
	rec.ResponseKey = responseKey
	rec.Status = store.IdempotencyCompleted
	s.idempotency[compositeKey] = rec
	return nil
}

func (s *Store) Release(ctx context.Context, principalID, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.idempotency, principalID+":"+key)
	return nil
}
