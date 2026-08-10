package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
)

// --- OpenTask ---

func (s *Store) PutOpenTask(ctx context.Context, t domain.OpenTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openTasks[t.ID] = t
	return nil
}

func (s *Store) GetOpenTask(ctx context.Context, id string) (domain.OpenTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.openTasks[id]
	if !ok {
		return domain.OpenTask{}, store.ErrNotFound
	}
	return t, nil
}

func (s *Store) OpenTaskByIdempotencyKey(ctx context.Context, principalID, key string) (domain.OpenTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.openTasks {
		if t.PrincipalID == principalID && t.PublicationIdempotencyKey == key {
			return t, nil
		}
	}
	return domain.OpenTask{}, store.ErrNotFound
}

func (s *Store) OpenTasksByPrincipal(ctx context.Context, principalID string) ([]domain.OpenTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.OpenTask
	for _, t := range s.openTasks {
		if t.PrincipalID == principalID {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) ListPublicOpenTasks(ctx context.Context, limit int) ([]domain.OpenTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.OpenTask
	for _, t := range s.openTasks {
		if t.Status == domain.OpenTaskOpen {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) UpdateOpenTask(ctx context.Context, id string, fn func(t domain.OpenTask, exists bool) (domain.OpenTask, error)) (domain.OpenTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.openTasks[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.OpenTask{}, err
	}
	s.openTasks[id] = next
	return next, nil
}

// --- OpenTaskProposal ---

func (s *Store) PutOpenTaskProposal(ctx context.Context, p domain.OpenTaskProposal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openTaskProposals[p.ID] = p
	return nil
}

func (s *Store) GetOpenTaskProposal(ctx context.Context, id string) (domain.OpenTaskProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.openTaskProposals[id]
	if !ok {
		return domain.OpenTaskProposal{}, store.ErrNotFound
	}
	return p, nil
}

func (s *Store) OpenTaskProposalByIdempotencyKey(ctx context.Context, providerID, key string) (domain.OpenTaskProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.openTaskProposals {
		if p.ProviderID == providerID && p.ProposalIdempotencyKey == key {
			return p, nil
		}
	}
	return domain.OpenTaskProposal{}, store.ErrNotFound
}

func (s *Store) ProposalsByTask(ctx context.Context, taskID string) ([]domain.OpenTaskProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.OpenTaskProposal
	for _, p := range s.openTaskProposals {
		if p.TaskID == taskID {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) UpdateOpenTaskProposal(ctx context.Context, id string, fn func(p domain.OpenTaskProposal, exists bool) (domain.OpenTaskProposal, error)) (domain.OpenTaskProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.openTaskProposals[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.OpenTaskProposal{}, err
	}
	if exists {
		if next.ID != current.ID || next.TaskID != current.TaskID || next.ProviderID != current.ProviderID ||
			next.CapabilityID != current.CapabilityID || next.CapabilityVersion != current.CapabilityVersion ||
			next.ProposalIdempotencyKey != current.ProposalIdempotencyKey {
			return domain.OpenTaskProposal{}, domain.NewError(domain.ErrIdempotencyConflict, "proposal update must not change identity fields", false)
		}
	}
	s.openTaskProposals[id] = next
	return next, nil
}

// --- AcceptanceOperation ---

// acceptanceOperationContentHash summarizes the identity fields that must
// never change once an AcceptanceOperation is opened -- mirrors
// signerOperationContentHash's role for domain.ExecutionSignerOperation.
func acceptanceOperationContentHash(op domain.AcceptanceOperation) string {
	encoded, _ := json.Marshal(struct {
		TaskID, ProposalID, PrincipalID, ProviderID string
		CapabilityID, CapabilityVersion             string
		IdempotencyKey                              string
	}{
		op.TaskID, op.ProposalID, op.PrincipalID, op.ProviderID,
		op.CapabilityID, op.CapabilityVersion,
		op.IdempotencyKey,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func acceptanceOpIdemKey(principalID, key string) string {
	return principalID + ":" + key
}

// hasNonTerminalAcceptanceOperationLocked reports whether a non-terminal
// AcceptanceOperation already exists for taskID -- the "at most one
// in-flight acceptance per task" invariant OpenAcceptanceOperation enforces.
// Caller must hold s.mu.
func (s *Store) hasNonTerminalAcceptanceOperationLocked(taskID string) bool {
	for _, op := range s.acceptanceOperations {
		if op.TaskID == taskID && !op.Checkpoint.Terminal() {
			return true
		}
	}
	return false
}

// OpenAcceptanceOperation mirrors OpenSignerOperationForCapability's exact
// ordering: the in-flight guard and build() run first, using the task's
// current state; the idempotency-key dedup/conflict check runs LAST,
// against the op build() just produced. This ordering is deliberate --
// build() depends on task.Status==Open, which becomes false the instant the
// first successful call claims the winner, so build() must never be the
// thing a genuine SEQUENTIAL replay (after the original call already
// completed) runs through: the caller (service.OpenTaskService.Accept) is
// responsible for checking AcceptanceOperationByIdempotencyKey FIRST and
// skipping this method entirely on a resume, exactly mirroring
// service.ExecutionSignerService.Authorize's resumeOrConflict-before-Open
// pattern. What this method's own final idempotency check defends against
// is the narrower TRUE-CONCURRENT race (two callers with the same key
// reaching here before either has completed) -- both compute build() from
// the same pre-completion snapshot and therefore produce identical content,
// so the loser's insert safely resolves to "return the winner's row" rather
// than a spurious conflict.
func (s *Store) OpenAcceptanceOperation(
	ctx context.Context, taskID string,
	build func(task domain.OpenTask) (domain.AcceptanceOperation, error),
) (domain.AcceptanceOperation, domain.OpenTask, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hasNonTerminalAcceptanceOperationLocked(taskID) {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, domain.NewError(domain.ErrOpenTaskAcceptanceInProgress,
			"an acceptance is already in progress for this open task", true)
	}

	task, ok := s.openTasks[taskID]
	if !ok {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, store.ErrNotFound
	}

	op, err := build(task)
	if err != nil {
		return domain.AcceptanceOperation{}, domain.OpenTask{}, false, err
	}

	idemKey := acceptanceOpIdemKey(op.PrincipalID, op.IdempotencyKey)
	if existingID, ok := s.acceptanceOpIdemIndex()[idemKey]; ok {
		existing := s.acceptanceOperations[existingID]
		if acceptanceOperationContentHash(existing) != acceptanceOperationContentHash(op) {
			return domain.AcceptanceOperation{}, domain.OpenTask{}, false, domain.NewError(domain.ErrIdempotencyConflict,
				"idempotency_key reused with different acceptance content", false)
		}
		return existing, s.openTasks[existing.TaskID], false, nil
	}

	s.acceptanceOperations[op.ID] = op
	task.AcceptedProposalID = op.ProposalID
	task.Status = domain.OpenTaskAccepted
	task.UpdatedAt = op.CreatedAt
	s.openTasks[taskID] = task
	return op, task, true, nil
}

// acceptanceOpIdemIndex rebuilds a (principalID:key -> opID) lookup on
// demand rather than maintaining a second map kept manually in sync with
// s.acceptanceOperations on every insert -- the in-memory store favors
// obvious correctness over raw throughput (see the package doc comment),
// and this map is bounded by the number of AcceptanceOperations that exist
// at all, which Phase 3C's own uniqueness invariants keep small (at most
// one non-terminal plus a handful of terminal attempts per OpenTask).
// Caller must hold s.mu.
func (s *Store) acceptanceOpIdemIndex() map[string]string {
	idx := make(map[string]string, len(s.acceptanceOperations))
	for id, op := range s.acceptanceOperations {
		idx[acceptanceOpIdemKey(op.PrincipalID, op.IdempotencyKey)] = id
	}
	return idx
}

func (s *Store) GetAcceptanceOperation(ctx context.Context, id string) (domain.AcceptanceOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.acceptanceOperations[id]
	if !ok {
		return domain.AcceptanceOperation{}, store.ErrNotFound
	}
	return op, nil
}

func (s *Store) AcceptanceOperationByIdempotencyKey(ctx context.Context, principalID, key string) (domain.AcceptanceOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.acceptanceOpIdemIndex()[acceptanceOpIdemKey(principalID, key)]
	if !ok {
		return domain.AcceptanceOperation{}, store.ErrNotFound
	}
	return s.acceptanceOperations[id], nil
}

func (s *Store) AcceptanceOperationByTask(ctx context.Context, taskID string) (domain.AcceptanceOperation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest domain.AcceptanceOperation
	found := false
	for _, op := range s.acceptanceOperations {
		if op.TaskID != taskID {
			continue
		}
		if !found || op.UpdatedAt.After(latest.UpdatedAt) {
			latest, found = op, true
		}
	}
	return latest, found, nil
}

func (s *Store) StaleAcceptanceOperations(ctx context.Context, cutoff time.Time, limit int) ([]domain.AcceptanceOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.AcceptanceOperation
	for _, op := range s.acceptanceOperations {
		if op.Checkpoint.Terminal() || op.UpdatedAt.After(cutoff) {
			continue
		}
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) UpdateAcceptanceOperation(ctx context.Context, id string, fn func(op domain.AcceptanceOperation, exists bool) (domain.AcceptanceOperation, error)) (domain.AcceptanceOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.acceptanceOperations[id]
	next, err := fn(current, exists)
	if err != nil {
		return domain.AcceptanceOperation{}, err
	}
	if exists {
		if next.ID != current.ID {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "acceptance operation update must not change the operation id", false)
		}
		if acceptanceOperationContentHash(current) != acceptanceOperationContentHash(next) {
			return domain.AcceptanceOperation{}, domain.NewError(domain.ErrIdempotencyConflict, "acceptance operation update must not change identity fields", false)
		}
	}
	s.acceptanceOperations[id] = next
	return next, nil
}
