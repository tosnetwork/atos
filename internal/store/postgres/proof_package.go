package postgres

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store"
	"time"
)

const proofCols = `id,receipt_id,job_id,quote_id,escrow_id,principal_id,semantic_digest,package_digest,canonical_cbor,checkpoint,last_error,created_at,updated_at,completed_at`

type scanner interface{ Scan(...any) error }

func scanProof(r scanner) (domain.ProofPackageOperation, error) {
	var o domain.ProofPackageOperation
	var c string
	e := r.Scan(&o.ID, &o.ReceiptID, &o.JobID, &o.QuoteID, &o.EscrowID, &o.PrincipalID, &o.SemanticDigest, &o.PackageDigest, &o.CanonicalCBOR, &c, &o.LastError, &o.CreatedAt, &o.UpdatedAt, &o.CompletedAt)
	o.Checkpoint = domain.ProofPackageCheckpoint(c)
	return o, e
}
func (s *Store) OpenProofPackageOperation(ctx context.Context, o domain.ProofPackageOperation) (domain.ProofPackageOperation, bool, error) {
	if o.CanonicalCBOR == nil {
		o.CanonicalCBOR = []byte{}
	}
	tx, e := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return o, false, e
	}
	defer tx.Rollback(ctx)
	if e = lockTransactionKey(ctx, tx, "proof-package", o.ReceiptID); e != nil {
		return o, false, e
	}
	existing, e := scanProof(tx.QueryRow(ctx, `SELECT `+proofCols+` FROM proof_package_operations WHERE receipt_id=$1`, o.ReceiptID))
	if e == nil {
		if existing.SemanticDigest != o.SemanticDigest {
			return o, false, domain.NewError(domain.ErrIdempotencyConflict, "proof identity reused with changed semantics", false)
		}
		return existing, false, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return o, false, e
	}
	_, e = tx.Exec(ctx, `INSERT INTO proof_package_operations (`+proofCols+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, o.ID, o.ReceiptID, o.JobID, o.QuoteID, o.EscrowID, o.PrincipalID, o.SemanticDigest, o.PackageDigest, o.CanonicalCBOR, string(o.Checkpoint), o.LastError, o.CreatedAt, o.UpdatedAt, o.CompletedAt)
	if e != nil {
		return o, false, e
	}
	return o, true, tx.Commit(ctx)
}
func (s *Store) GetProofPackageOperation(ctx context.Context, id string) (domain.ProofPackageOperation, error) {
	o, e := scanProof(s.pool.QueryRow(ctx, `SELECT `+proofCols+` FROM proof_package_operations WHERE id=$1`, id))
	if errors.Is(e, pgx.ErrNoRows) {
		e = store.ErrNotFound
	}
	return o, e
}
func (s *Store) ProofPackageOperationByReceipt(ctx context.Context, id string) (domain.ProofPackageOperation, error) {
	o, e := scanProof(s.pool.QueryRow(ctx, `SELECT `+proofCols+` FROM proof_package_operations WHERE receipt_id=$1`, id))
	if errors.Is(e, pgx.ErrNoRows) {
		e = store.ErrNotFound
	}
	return o, e
}
func (s *Store) UpdateProofPackageOperation(ctx context.Context, id string, fn func(domain.ProofPackageOperation) (domain.ProofPackageOperation, error)) (domain.ProofPackageOperation, error) {
	tx, e := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return domain.ProofPackageOperation{}, e
	}
	defer tx.Rollback(ctx)
	old, e := scanProof(tx.QueryRow(ctx, `SELECT `+proofCols+` FROM proof_package_operations WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(e, pgx.ErrNoRows) {
		return old, store.ErrNotFound
	}
	if e != nil {
		return old, e
	}
	next, e := fn(old)
	if e != nil {
		return old, e
	}
	if next.ID != old.ID || next.ReceiptID != old.ReceiptID || next.JobID!=old.JobID||next.QuoteID!=old.QuoteID||next.EscrowID!=old.EscrowID||next.PrincipalID!=old.PrincipalID||next.SemanticDigest != old.SemanticDigest || (old.PackageDigest!=""&&next.PackageDigest!=old.PackageDigest) || !old.Checkpoint.CanAdvance(next.Checkpoint) {
		return old, store.ErrConflict
	}
	tag, e := tx.Exec(ctx, `UPDATE proof_package_operations SET package_digest=$2,canonical_cbor=$3,checkpoint=$4,last_error=$5,updated_at=$6,completed_at=$7 WHERE id=$1 AND checkpoint=$8`, id, next.PackageDigest, next.CanonicalCBOR, string(next.Checkpoint), next.LastError, next.UpdatedAt, next.CompletedAt, string(old.Checkpoint))
	if e != nil {
		return old, e
	}
	if tag.RowsAffected() != 1 {
		return old, store.ErrConflict
	}
	return next, tx.Commit(ctx)
}
func (s *Store) StaleProofPackageOperations(ctx context.Context, cut time.Time, limit int) ([]domain.ProofPackageOperation, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, e := s.pool.Query(ctx, `SELECT `+proofCols+` FROM proof_package_operations WHERE checkpoint<>'completed' AND updated_at<$1 ORDER BY updated_at,id LIMIT $2`, cut, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []domain.ProofPackageOperation
	for rows.Next() {
		o, e := scanProof(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
