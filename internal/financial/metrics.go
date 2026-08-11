package financial

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// IntegrityMetrics is deliberately aggregate and contains no account,
// transaction, payment-rail, credential, or payload identity.
type IntegrityMetrics struct {
	SafeMode                  bool
	LedgerCommitFailures      int64
	ReconciliationMismatches  int64
	LastFinalizedSequence     int64
	LastSignedBatchSequence   int64
	LastAnchoredBatchSequence int64
	PendingIntents            int64
	UnresolvedIncidents       int64
	AnchorLagSeconds          float64
	PayoutLagSeconds          float64
}

func (r *Repository) IntegrityMetrics(ctx context.Context) (IntegrityMetrics, error) {
	var value IntegrityMetrics
	err := r.pool.QueryRow(ctx, `SELECT
  (SELECT safe_mode FROM financial_integrity_state WHERE singleton=TRUE),
  (SELECT count(*) FROM financial_events WHERE state<>'finalized' AND last_error<>''),
  (SELECT count(*) FROM financial_integrity_incidents WHERE classification='financial_reconciliation_mismatch'),
  (SELECT COALESCE(max(sequence),0) FROM financial_events WHERE state='finalized'),
  (SELECT COALESCE(max(batch_sequence),0) FROM financial_batches WHERE state IN ('signed','retained','anchored')),
  (SELECT COALESCE(max(batch_sequence),0) FROM financial_batches WHERE state='anchored'),
  (SELECT count(*) FROM financial_events WHERE state IN ('intent','submitting')),
  (SELECT count(*) FROM financial_integrity_incidents WHERE resolved_at IS NULL),
  (SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(created_at)),0) FROM financial_batches WHERE state<>'anchored'),
  (SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(payout_requested_at)),0) FROM provider_earnings WHERE status='payout_pending')`).Scan(
		&value.SafeMode, &value.LedgerCommitFailures, &value.ReconciliationMismatches,
		&value.LastFinalizedSequence, &value.LastSignedBatchSequence, &value.LastAnchoredBatchSequence,
		&value.PendingIntents, &value.UnresolvedIncidents, &value.AnchorLagSeconds, &value.PayoutLagSeconds,
	)
	return value, err
}

// MetricsHandler exposes Prometheus text without adding a mutable metrics
// registry to the financial correctness path. Every value is reconstructed
// from durable checkpoints at scrape time.
func MetricsHandler(repository *Repository, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), timeout)
		defer cancel()
		metrics, err := repository.IntegrityMetrics(ctx)
		if err != nil {
			http.Error(response, "financial metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		safeMode := 0
		if metrics.SafeMode {
			safeMode = 1
		}
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(response, `# TYPE atos_financial_safe_mode gauge
atos_financial_safe_mode %d
# TYPE atos_financial_ledger_commit_failures gauge
atos_financial_ledger_commit_failures %d
# TYPE atos_financial_reconciliation_mismatches counter
atos_financial_reconciliation_mismatches %d
# TYPE atos_financial_last_finalized_sequence gauge
atos_financial_last_finalized_sequence %d
# TYPE atos_financial_last_signed_batch_sequence gauge
atos_financial_last_signed_batch_sequence %d
# TYPE atos_financial_last_anchored_batch_sequence gauge
atos_financial_last_anchored_batch_sequence %d
# TYPE atos_financial_pending_intents gauge
atos_financial_pending_intents %d
# TYPE atos_financial_unresolved_incidents gauge
atos_financial_unresolved_incidents %d
# TYPE atos_financial_anchor_lag_seconds gauge
atos_financial_anchor_lag_seconds %.0f
# TYPE atos_financial_payout_reconciliation_lag_seconds gauge
atos_financial_payout_reconciliation_lag_seconds %.0f
`, safeMode, metrics.LedgerCommitFailures, metrics.ReconciliationMismatches,
			metrics.LastFinalizedSequence, metrics.LastSignedBatchSequence, metrics.LastAnchoredBatchSequence,
			metrics.PendingIntents, metrics.UnresolvedIncidents, metrics.AnchorLagSeconds, metrics.PayoutLagSeconds)
	})
}
