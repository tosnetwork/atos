package domain

import "time"

type ProofPackageCheckpoint string

const (
	ProofPackageIntentPersisted     ProofPackageCheckpoint = "intent_persisted"
	ProofPackageReconciling         ProofPackageCheckpoint = "reconciling"
	ProofPackageCanonicalObserved   ProofPackageCheckpoint = "canonical_observed"
	ProofPackageProjectionPersisted ProofPackageCheckpoint = "projection_persisted"
	ProofPackageCompleted           ProofPackageCheckpoint = "completed"
)

func (c ProofPackageCheckpoint) Terminal() bool { return c == ProofPackageCompleted }
func (c ProofPackageCheckpoint) CanAdvance(n ProofPackageCheckpoint) bool {
	if c == n {
		return true
	}
	if c.Terminal() {
		return false
	}
	order := map[ProofPackageCheckpoint]int{ProofPackageIntentPersisted: 1, ProofPackageReconciling: 2, ProofPackageCanonicalObserved: 3, ProofPackageProjectionPersisted: 4, ProofPackageCompleted: 5}
	return order[n] >= order[c] && order[n] != 0
}

type ProofPackageOperation struct {
	ID, ReceiptID, JobID, QuoteID, EscrowID, PrincipalID, SemanticDigest, PackageDigest string
	CanonicalCBOR                                                                       []byte
	Checkpoint                                                                          ProofPackageCheckpoint
	LastError                                                                           string
	CreatedAt, UpdatedAt                                                                time.Time
	CompletedAt                                                                         *time.Time
}
