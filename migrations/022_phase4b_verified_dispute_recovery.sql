-- Phase 4B Verified dispute mutation/projection recovery.  The existing
-- disputes payload carries the frozen canonical tuple; dedicated lifecycle
-- columns provide durable, fair recovery scans shared by all replicas.
DROP INDEX IF EXISTS idx_disputes_recovery;
CREATE INDEX idx_disputes_recovery
    ON disputes (updated_at ASC, id ASC)
    WHERE economic_state IN (
        'pending_payout_resolution',
        'verified_open_pending',
        'verified_resolution_pending'
    );
