// Package payout defines ATOS's boundary to an external provider-payout
// rail. It describes only the side effect of moving funds out of ATOS to a
// provider -- never the durable ledger state that records it, which lives
// in domain.ProviderEarning / store.Earnings. internal/service/earnings.go
// is responsible for sequencing "commit a durable local intent -> call this
// adapter -> commit a durable local completion checkpoint" around it; no
// other caller should invoke an Adapter directly.
package payout

import (
	"context"

	"github.com/tosnetwork/atos/internal/domain"
)

// Status is the outcome of one payout attempt.
type Status string

const (
	// StatusPaid means the rail confirms funds were moved, with a durable
	// Reference identifying the transfer.
	StatusPaid Status = "paid"
	// StatusPending means the outcome is not yet known: the call may still
	// be in flight, or the rail is asynchronous and has not settled.
	// Callers MUST treat this as "unknown, not failed" -- the earning
	// stays in payout_pending, and only a later replay using the SAME
	// IdempotencyKey may learn the final outcome. Never infer success or
	// failure from a StatusPending result.
	StatusPending Status = "pending"
	// StatusRejected means the rail definitively and safely refused the
	// payout before any funds moved (e.g. an invalid destination) and
	// guarantees no side effect occurred for this idempotency key. It is
	// the only status a caller may treat as "did not happen" without a
	// durable completion checkpoint proving so.
	StatusRejected Status = "rejected"
)

// Request describes one payout attempt. IdempotencyKey is the stable
// external identity for this exact payout. An Adapter implementation MUST
// guarantee that any number of calls sharing the same IdempotencyKey either
// all observe the same terminal Result, or safely report StatusPending --
// and MUST NEVER move funds twice for one IdempotencyKey.
type Request struct {
	IdempotencyKey string
	EarningID      string
	ProviderID     string
	Amount         domain.Money
}

// Result is the outcome of a Payout or Query call.
type Result struct {
	Status    Status
	Reference string // set only when Status == StatusPaid: the rail's durable transfer identifier
	Reason    string // human-readable detail, set for StatusRejected/StatusPending
}

// Adapter is ATOS's boundary to an external payout rail (bank transfer,
// stablecoin transfer, PSP payout API, ...). Every method must be safe to
// call any number of times with the same IdempotencyKey.
type Adapter interface {
	// Payout attempts, or on retry replays, a payout for req. It is the
	// only method that may cause an external side effect.
	Payout(ctx context.Context, req Request) (Result, error)
	// Query looks up the outcome of a previously attempted payout by its
	// idempotency key without causing any new side effect. found=false
	// means the rail has no record of ever attempting this key -- a safe
	// signal that Payout has never been called, or never successfully
	// reached the rail, for it.
	Query(ctx context.Context, idempotencyKey string) (result Result, found bool, err error)
}
