package financial

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tosnetwork/atos/migrations"
)

type testLedger struct {
	mu           sync.Mutex
	transactions map[string]LedgerTransaction
	balances     map[string]int64
	submitCalls  int
	lostOnce     bool
	blockLookup  string
	lookupStart  chan struct{}
	lookupResume chan struct{}
}

func newTestLedger() *testLedger {
	return &testLedger{transactions: map[string]LedgerTransaction{}, balances: map[string]int64{}}
}
func (l *testLedger) Submit(_ context.Context, e Event, allow bool) (LedgerTransaction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.submitCalls++
	if tx, ok := l.transactions[e.LedgerReference]; ok {
		return tx, nil
	}
	var amount int64
	fmt.Sscan(e.AtomicAmount, &amount)
	if !allow && l.balances[e.SourceIndicator] < amount {
		return LedgerTransaction{}, errors.New("insufficient")
	}
	l.balances[e.SourceIndicator] -= amount
	l.balances[e.DestinationIndicator] += amount
	tx := LedgerTransaction{TransactionID: e.LedgerTransactionID, Source: e.SourceIndicator, Destination: e.DestinationIndicator, Reference: e.LedgerReference, PreciseAmount: json.Number(e.AtomicAmount), Currency: e.Asset, Description: "atos-financial-v1:" + e.Digest, Status: "APPLIED"}
	l.transactions[e.LedgerReference] = tx
	if l.lostOnce {
		l.lostOnce = false
		return LedgerTransaction{}, errors.New("injected lost response")
	}
	return tx, nil
}
func (l *testLedger) Lookup(_ context.Context, ref string) (LedgerTransaction, bool, error) {
	l.mu.Lock()
	tx, ok := l.transactions[ref]
	block := ref == l.blockLookup && l.lookupStart != nil && l.lookupResume != nil
	started, resume := l.lookupStart, l.lookupResume
	if block {
		l.blockLookup = ""
	}
	l.mu.Unlock()
	if block {
		close(started)
		<-resume
	}
	return tx, ok, nil
}
func (l *testLedger) Balance(_ context.Context, indicator, _ string) (string, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	value, ok := l.balances[indicator]
	return fmt.Sprint(value), ok, nil
}
func (l *testLedger) Verify(_ context.Context, e Event, tx LedgerTransaction) error {
	if tx.TransactionID != e.LedgerTransactionID || tx.Reference != e.LedgerReference || tx.PreciseAmount.String() != e.AtomicAmount || tx.Currency != e.Asset || tx.Description != "atos-financial-v1:"+e.Digest || tx.Status != "APPLIED" || tx.Source != e.SourceIndicator || tx.Destination != e.DestinationIndicator {
		return ErrIdempotencyConflict
	}
	return nil
}

func financialTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ATOS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ATOS_TEST_DATABASE_URL is required for financial PostgreSQL integration")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE financial_events,financial_projections,financial_integrity_incidents,financial_batches,financial_reconciler_leases`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_chain_state SET next_sequence=1,last_commitment=$1,next_batch_sequence=1,last_batch_id='',last_batch_root=$1,last_anchor_id=''`, GenesisDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_integrity_state SET safe_mode=FALSE,reason='',incident_id='',entered_at=NULL`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func requestFor(id string, event EventType, source, destination AccountCode, sourceOwner, destinationOwner string) TransferRequest {
	principalID := sourceOwner
	if event == EventAccountGenesis || event == EventEscrowRelease || event == EventSettlementRefund {
		principalID = destinationOwner
	}
	return TransferRequest{EventType: event, IdempotencyIdentity: id, Identities: Identities{PrincipalID: principalID, ProviderID: "provider_" + id, JobID: "job_" + id, QuoteID: "quote_" + id, CapabilityID: "cap_" + id, CapabilityVersion: "1", BillingSnapshotID: "billing_" + id, ExecutionReceiptID: "execution_" + id, SettlementID: "settlement_" + id, ProviderEarningID: "earning_" + id, DisputeID: "dispute_" + id, PayoutID: "payout_" + id}, Asset: "USD", Decimals: 2, AtomicAmount: "1000", SourceCode: source, SourceOwnerID: sourceOwner, DestinationCode: destination, DestinationOwnerID: destinationOwner}
}

func TestFinancialMetricsExposeOnlyAggregateDurableState(t *testing.T) {
	pool := financialTestPool(t)
	repository, err := NewRepository(pool, "gateway-metrics", "network-metrics")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/internal/financial-integrity/metrics", nil)
	response := httptest.NewRecorder()
	MetricsHandler(repository, time.Second).ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("metrics response status=%d body=%s", response.Code, response.Body.String())
	}
	body, _ := io.ReadAll(response.Body)
	for _, name := range []string{"atos_financial_safe_mode", "atos_financial_last_finalized_sequence", "atos_financial_last_anchored_batch_sequence", "atos_financial_payout_reconciliation_lag_seconds"} {
		if !strings.Contains(string(body), name) {
			t.Fatalf("metrics response missing %s", name)
		}
	}
	if strings.Contains(string(body), "gateway-metrics") || strings.Contains(string(body), "network-metrics") {
		t.Fatal("metrics exposed financial identity")
	}
}

func TestPostgresTwoReplicaIdempotencyAndLostResponse(t *testing.T) {
	ctx := context.Background()
	pool1 := financialTestPool(t)
	pool2, err := pgxpool.New(ctx, os.Getenv("ATOS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	gateway := "gw-" + suffix
	network := "net-" + suffix
	r1, _ := NewRepository(pool1, gateway, network)
	r2, _ := NewRepository(pool2, gateway, network)
	ledger := newTestLedger()
	a1, _ := NewAdapter(r1, ledger)
	a2, _ := NewAdapter(r2, ledger)
	principal := "principal-" + suffix
	genesis := requestFor("genesis-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	genesis.AllowOverdraft = true
	genesis.AtomicAmount = "1000"
	if _, err := a1.ProvisionAccount(ctx, genesis); err != nil {
		t.Fatal(err)
	}
	reserve := requestFor("reserve-"+suffix, EventReserve, PrincipalAvailable, PrincipalReserved, principal, principal)
	const replicas = 24
	errorsOut := make(chan error, replicas)
	var wg sync.WaitGroup
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			adapter := a1
			if i%2 == 1 {
				adapter = a2
			}
			_, err := adapter.Reserve(ctx, reserve)
			errorsOut <- err
		}(i)
	}
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatalf("same semantic retry failed: %v", err)
		}
	}
	ledger.mu.Lock()
	calls := ledger.submitCalls
	ledger.mu.Unlock()
	if calls != 2 {
		t.Fatalf("ledger submission count=%d want genesis+one reserve", calls)
	}
	changed := reserve
	changed.AtomicAmount = "999"
	if _, err := a2.Reserve(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed semantics error=%v", err)
	}
	ledger.lostOnce = true
	release := requestFor("release-"+suffix, EventReservationRelease, PrincipalReserved, PrincipalAvailable, principal, principal)
	if _, err := a1.ReleaseReservation(ctx, release); err != nil {
		t.Fatalf("lost response did not converge: %v", err)
	}
	event, err := a2.Lookup(ctx, release.IdempotencyIdentity)
	if err != nil || event.State != "finalized" {
		t.Fatalf("recovered event=%+v err=%v", event, err)
	}
}

func TestConcurrentDistinctMutationsDoNotStarvePool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = financialTestPool(t)
	configuration, err := pgxpool.ParseConfig(os.Getenv("ATOS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	configuration.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	repository, _ := NewRepository(pool, "gw-"+suffix, "net-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repository, ledger)
	principal := "principal-" + suffix
	genesis := requestFor("pool-genesis-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	genesis.AllowOverdraft = true
	genesis.AtomicAmount = "1000"
	if _, err := adapter.ProvisionAccount(ctx, genesis); err != nil {
		t.Fatal(err)
	}
	const mutations = 16
	errorsOut := make(chan error, mutations)
	var wait sync.WaitGroup
	for index := 0; index < mutations; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			request := requestFor(fmt.Sprintf("pool-reserve-%s-%d", suffix, index), EventReserve, PrincipalAvailable, PrincipalReserved, principal, principal)
			request.AtomicAmount = "1"
			_, mutationErr := adapter.Reserve(ctx, request)
			errorsOut <- mutationErr
		}(index)
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatalf("distinct concurrent mutation starved or failed: %v", err)
		}
	}
}

func TestReconciliationCompletesWithSingleConnectionPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = financialTestPool(t)
	configuration, err := pgxpool.ParseConfig(os.Getenv("ATOS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	configuration.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	suffix := fmt.Sprint(time.Now().UnixNano())
	repository, _ := NewRepository(pool, "gw-"+suffix, "net-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repository, ledger)
	principal := "principal-" + suffix
	genesis := requestFor("single-pool-genesis-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	genesis.AllowOverdraft = true
	if _, err := adapter.ProvisionAccount(ctx, genesis); err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Reconcile(ctx, 100)
	if err != nil || result.SafeMode || result.Mismatches != 0 {
		t.Fatalf("single-connection reconciliation result=%+v err=%v", result, err)
	}
}

func TestCompensatingReversalExactlyInvertsOneFinalizedEvent(t *testing.T) {
	ctx := context.Background()
	pool := financialTestPool(t)
	suffix := fmt.Sprint(time.Now().UnixNano())
	repository, _ := NewRepository(pool, "gw-"+suffix, "net-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repository, ledger)
	principal := "principal-" + suffix
	genesis := requestFor("genesis-reversal-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	genesis.AtomicAmount = "1000"
	genesis.AllowOverdraft = true
	if _, err := adapter.ProvisionAccount(ctx, genesis); err != nil {
		t.Fatal(err)
	}
	reserveRequest := requestFor("reserve-reversal-"+suffix, EventReserve, PrincipalAvailable, PrincipalReserved, principal, principal)
	reserveRequest.AtomicAmount = "1000"
	reserved, err := adapter.Reserve(ctx, reserveRequest)
	if err != nil {
		t.Fatal(err)
	}

	reversal := requestFor("reverse-once-"+suffix, EventCompensatingReversal, PrincipalReserved, PrincipalAvailable, principal, principal)
	reversal.AtomicAmount = reserved.AtomicAmount
	reversal.ReversesEventID = reserved.EventID
	changed := reversal
	changed.IdempotencyIdentity = "reverse-wrong-amount-" + suffix
	changed.AtomicAmount = "999"
	if _, err := adapter.CompensatingReversal(ctx, changed); err == nil {
		t.Fatal("changed-amount reversal was accepted")
	}
	if _, err := adapter.CompensatingReversal(ctx, reversal); err != nil {
		t.Fatalf("exact reversal failed: %v", err)
	}
	second := reversal
	second.IdempotencyIdentity = "reverse-twice-" + suffix
	if _, err := adapter.CompensatingReversal(ctx, second); err == nil {
		t.Fatal("same finalized event was reversed twice")
	}
}

func TestFinalizedEvidenceDatabaseMutationIsRejected(t *testing.T) {
	ctx := context.Background()
	pool := financialTestPool(t)
	suffix := fmt.Sprint(time.Now().UnixNano())
	repo, _ := NewRepository(pool, "gw-"+suffix, "net-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repo, ledger)
	principal := "p-" + suffix
	request := requestFor("immutable-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	request.AllowOverdraft = true
	event, err := adapter.ProvisionAccount(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_events SET atomic_amount=atomic_amount+1 WHERE idempotency_identity=$1`, request.IdempotencyIdentity); err == nil {
		t.Fatal("sealed event amount mutation succeeded")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM financial_events WHERE idempotency_identity=$1`, request.IdempotencyIdentity); err == nil {
		t.Fatal("sealed event deletion succeeded")
	}
	if got, err := repo.Lookup(ctx, request.IdempotencyIdentity); err != nil || got.Digest != event.Digest {
		t.Fatalf("evidence changed after mutation attempts: %+v %v", got, err)
	}
}

func TestReconciliationMismatchEntersSafeMode(t *testing.T) {
	ctx := context.Background()
	pool := financialTestPool(t)
	suffix := fmt.Sprint(time.Now().UnixNano())
	repo, _ := NewRepository(pool, "gw-"+suffix, "net-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repo, ledger)
	principal := "p-" + suffix
	request := requestFor("reconcile-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	request.AllowOverdraft = true
	if _, err := adapter.ProvisionAccount(ctx, request); err != nil {
		t.Fatal(err)
	}
	if result, err := adapter.Reconcile(ctx, 100); err != nil || result.SafeMode || result.Mismatches != 0 {
		t.Fatalf("clean reconciliation result=%+v err=%v", result, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_projections SET atomic_balance=atomic_balance+1 WHERE account_code=$1 AND account_owner_id=$2`, PrincipalAvailable, principal); err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Reconcile(ctx, 100)
	if err == nil || !result.SafeMode || result.Mismatches != 1 {
		t.Fatalf("corrupt reconciliation result=%+v err=%v", result, err)
	}
	if err := adapter.RebuildProjections(ctx); err != nil {
		t.Fatalf("deterministic projection rebuild: %v", err)
	}
	var rebuilt string
	if err := pool.QueryRow(ctx, `SELECT atomic_balance::text FROM financial_projections WHERE account_code=$1 AND account_owner_id=$2`, PrincipalAvailable, principal).Scan(&rebuilt); err != nil || rebuilt != "1000" {
		t.Fatalf("rebuilt balance=%s err=%v", rebuilt, err)
	}
	if _, err := adapter.Reserve(ctx, requestFor("blocked-"+suffix, EventReserve, PrincipalAvailable, PrincipalReserved, principal, principal)); !errors.Is(err, ErrSafeMode) {
		t.Fatalf("safe mode write error=%v", err)
	}
}

func TestSafeModeObservesCommittedPendingButNeverSubmitsAbsentTransaction(t *testing.T) {
	ctx := context.Background()
	pool := financialTestPool(t)
	suffix := fmt.Sprint(time.Now().UnixNano())
	repository, _ := NewRepository(pool, "gw-"+suffix, "net-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repository, ledger)
	principal := "p-" + suffix

	absentRequest := requestFor("safe-absent-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	absentRequest.AllowOverdraft = true
	absent, err := repository.OpenIntent(ctx, absentRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.EnterSafeMode(ctx, "test_safe_mode", map[string]any{"expected": true}, map[string]any{"observed": false}); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ProvisionAccount(ctx, absentRequest); !errors.Is(err, ErrSafeMode) {
		t.Fatalf("safe mode absent transaction error=%v", err)
	}
	if _, found, err := ledger.Lookup(ctx, absent.LedgerReference); err != nil || found {
		t.Fatalf("safe mode created absent ledger transaction: found=%t err=%v", found, err)
	}
	if result, err := adapter.Reconcile(ctx, 100); err != nil || !result.SafeMode || result.Retried != 0 {
		t.Fatalf("safe-mode reconcile submitted pending transaction: result=%+v err=%v", result, err)
	}

	committedRequest := requestFor("safe-committed-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", "committed-"+principal)
	committedRequest.AllowOverdraft = true
	// Safe mode forbids opening a new intent, so create this pending intent by
	// temporarily using the migration credential to model a pre-incident intent.
	if _, err := pool.Exec(ctx, `UPDATE financial_integrity_state SET safe_mode=FALSE,reason='',incident_id='',entered_at=NULL`); err != nil {
		t.Fatal(err)
	}
	committed, err := repository.OpenIntent(ctx, committedRequest)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := ledger.Submit(ctx, committed, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.EnterSafeMode(ctx, "test_safe_mode_again", map[string]any{"expected": true}, map[string]any{"observed": false}); err != nil {
		t.Fatal(err)
	}
	observed, err := adapter.ProvisionAccount(ctx, committedRequest)
	if err != nil || observed.State != "finalized" || observed.LedgerTransactionID != transaction.TransactionID {
		t.Fatalf("safe mode did not observe already committed outcome: event=%+v err=%v", observed, err)
	}
}

func TestReconciliationSerializesMutationAndPersistsCrashCursor(t *testing.T) {
	ctx := context.Background()
	pool := financialTestPool(t)
	suffix := fmt.Sprint(time.Now().UnixNano())
	repo, _ := NewRepository(pool, "gw-"+suffix, "net-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repo, ledger)
	principal := "p-" + suffix
	genesisRequest := requestFor("reconcile-lock-genesis-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", principal)
	genesisRequest.AllowOverdraft = true
	genesis, err := adapter.ProvisionAccount(ctx, genesisRequest)
	if err != nil {
		t.Fatal(err)
	}
	ledger.mu.Lock()
	ledger.blockLookup = genesis.LedgerReference
	ledger.lookupStart = make(chan struct{})
	ledger.lookupResume = make(chan struct{})
	started, resume := ledger.lookupStart, ledger.lookupResume
	ledger.mu.Unlock()

	reconcileDone := make(chan error, 1)
	go func() {
		_, reconcileErr := adapter.Reconcile(ctx, 100)
		reconcileDone <- reconcileErr
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not reach the injected ledger lookup")
	}

	reserveRequest := requestFor("reconcile-lock-reserve-"+suffix, EventReserve, PrincipalAvailable, PrincipalReserved, principal, principal)
	reserveDone := make(chan error, 1)
	go func() {
		_, reserveErr := adapter.Reserve(ctx, reserveRequest)
		reserveDone <- reserveErr
	}()
	select {
	case err := <-reserveDone:
		t.Fatalf("financial mutation bypassed active reconciliation: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := repo.Lookup(ctx, reserveRequest.IdempotencyIdentity); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("blocked mutation created an intent during reconciliation: %v", err)
	}
	close(resume)
	if err := <-reconcileDone; err != nil {
		t.Fatalf("reconciliation failed: %v", err)
	}
	if err := <-reserveDone; err != nil {
		t.Fatalf("mutation did not resume after reconciliation: %v", err)
	}
	var cursor int64
	var leaseExpired bool
	if err := pool.QueryRow(ctx, `SELECT cursor,lease_until<=now() FROM financial_reconciler_leases WHERE lease_name=$1`, reconciliationLeaseName).Scan(&cursor, &leaseExpired); err != nil {
		t.Fatal(err)
	}
	if cursor != 1 || !leaseExpired {
		t.Fatalf("durable reconciliation checkpoint cursor=%d lease_expired=%t", cursor, leaseExpired)
	}
}
