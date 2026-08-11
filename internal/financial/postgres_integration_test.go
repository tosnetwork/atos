package financial

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	tx := LedgerTransaction{TransactionID: e.LedgerTransactionID, Source: e.SourceIndicator, Destination: e.DestinationIndicator,
		SourceIndicator: e.SourceIndicator, DestinationIndicator: e.DestinationIndicator, Reference: e.LedgerReference,
		PreciseAmount: json.Number(e.AtomicAmount), Currency: e.Asset, Description: "atos-financial-v1:" + e.Digest,
		Status: "APPLIED", CreatedAt: time.Now().UTC()}
	l.transactions[e.LedgerReference] = tx
	if l.lostOnce {
		l.lostOnce = false
		return LedgerTransaction{}, errors.New("injected lost response")
	}
	return tx, nil
}

type staticChainLedger struct {
	*testLedger
	evidence LedgerChainEvidence
}

func (l *staticChainLedger) ChainEvidence(context.Context) (LedgerChainEvidence, error) {
	return l.evidence, nil
}

type countingSigner struct {
	private ed25519.PrivateKey
	public  string
	calls   atomic.Int32
}

func newCountingSigner(t *testing.T) *countingSigner {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &countingSigner{private: private, public: base64.StdEncoding.EncodeToString(public)}
}

func (s *countingSigner) Sign(_ context.Context, request SignRequest) (SignResponse, error) {
	s.calls.Add(1)
	digest, err := DigestBytes(request.Digest)
	if err != nil {
		return SignResponse{}, err
	}
	return SignResponse{KeyID: request.KeyID, Algorithm: request.Algorithm,
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(s.private, digest)), PublicKey: s.public,
		SignedUnixMillis: time.Now().UTC().UnixMilli()}, nil
}

type retainedTestObject struct {
	version string
	digest  string
}

type blockingRetainer struct {
	mu        sync.Mutex
	objects   map[string]retainedTestObject
	putCalls  int
	firstPut  sync.Once
	entered   chan struct{}
	release   chan struct{}
	retention time.Duration
}

func newBlockingRetainer() *blockingRetainer {
	return &blockingRetainer{objects: map[string]retainedTestObject{}, entered: make(chan struct{}), release: make(chan struct{}), retention: time.Hour}
}

func (r *blockingRetainer) MinimumRetention() time.Duration { return r.retention }

func (r *blockingRetainer) PutIfAbsent(_ context.Context, key string, _ []byte, digest string) (string, error) {
	r.firstPut.Do(func() {
		close(r.entered)
		<-r.release
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.putCalls++
	if object, ok := r.objects[key]; ok {
		if object.digest != digest {
			return "", ErrIdempotencyConflict
		}
		return object.version, nil
	}
	version := fmt.Sprintf("locked-version-%d", len(r.objects)+1)
	r.objects[key] = retainedTestObject{version: version, digest: digest}
	return version, nil
}

func (r *blockingRetainer) ResolveRetention(_ context.Context, key, version, digest string) (RetentionProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	object, ok := r.objects[key]
	if !ok || object.version != version || object.digest != digest {
		return RetentionProof{}, ErrIdempotencyConflict
	}
	return RetentionProof{ObjectKey: key, VersionID: version, Digest: digest, LockMode: "COMPLIANCE", RetainUntil: time.Now().UTC().Add(2 * r.retention)}, nil
}

type countingAnchorPublisher struct {
	mu      sync.Mutex
	receipt AnchorReceipt
	found   bool
	count   int
}

func (p *countingAnchorPublisher) ResolveManagedFinancialAnchor(_ context.Context, anchor ManagedAnchor) (AnchorReceipt, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.found {
		return AnchorReceipt{}, false, nil
	}
	if p.receipt.Anchor != anchor {
		return AnchorReceipt{}, false, ErrIdempotencyConflict
	}
	return p.receipt, true, nil
}

func (p *countingAnchorPublisher) PublishManagedFinancialAnchor(_ context.Context, anchor ManagedAnchor) (AnchorReceipt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count++
	digest, err := AnchorPayloadDigest(anchor)
	if err != nil {
		return AnchorReceipt{}, err
	}
	p.receipt = AnchorReceipt{Anchor: anchor, PayloadDigest: digest, NetworkReferenceID: "tos:test:" + anchor.AnchorID,
		NetworkID: anchor.NetworkID, Finalized: true, FinalizedCheckpoint: 1}
	p.found = true
	return p.receipt, nil
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

func TestRetentionDeadlinesAreDurableAndReplicaStable(t *testing.T) {
	ctx := context.Background()
	pool1 := financialTestPool(t)
	pool2, err := pgxpool.New(ctx, os.Getenv("ATOS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	repository1, _ := NewRepository(pool1, "gw-retention", "net-retention")
	repository2, _ := NewRepository(pool2, "gw-retention", "net-retention")
	batchID := fmt.Sprintf("fbat_retention_%d", time.Now().UnixNano())
	digest := "sha256:" + strings.Repeat("1", 64)
	if _, err := pool1.Exec(ctx, `INSERT INTO financial_batches
 (batch_id,batch_sequence,first_sequence,last_sequence,commitment_count,previous_batch_id,
  previous_merkle_root,merkle_root,manifest_digest,manifest_cbor,manifest,state,created_at)
 VALUES ($1,1,1,1,1,'',$2,$2,$3,'\\x00','{}','signed',now())`, batchID, GenesisDigest, digest); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	deadlines := make(chan time.Time, 2)
	errorsOut := make(chan error, 2)
	for _, repository := range []*Repository{repository1, repository2} {
		go func(repository *Repository) {
			<-start
			deadline, deadlineErr := repository.batchRetentionDeadline(ctx, batchID, 365*24*time.Hour, false)
			deadlines <- deadline
			errorsOut <- deadlineErr
		}(repository)
	}
	close(start)
	first, second := <-deadlines, <-deadlines
	for range 2 {
		if err := <-errorsOut; err != nil {
			t.Fatal(err)
		}
	}
	if !first.Equal(second) {
		t.Fatalf("replicas allocated different retention deadlines: %s != %s", first, second)
	}
	if _, err := pool1.Exec(ctx, `UPDATE financial_batches SET required_retain_until=required_retain_until+interval '1 second' WHERE batch_id=$1`, batchID); err == nil {
		t.Fatal("durable retention deadline remained mutable")
	}

	anchorBatchID := batchID + "_anchor"
	anchorDigest := "sha256:" + strings.Repeat("2", 64)
	if _, err := pool1.Exec(ctx, `INSERT INTO financial_batches
 (batch_id,batch_sequence,first_sequence,last_sequence,commitment_count,previous_batch_id,
  previous_merkle_root,merkle_root,manifest_digest,manifest_cbor,manifest,state,created_at,
  required_retain_until,retained_object_key,retained_version_id)
 VALUES ($1,2,2,2,1,$2,$3,$3,$4,'\\x00','{}','retained',now(),now()+interval '365 days','evidence','version')`,
		anchorBatchID, batchID, GenesisDigest, anchorDigest); err != nil {
		t.Fatal(err)
	}
	anchorDeadline, err := repository1.batchRetentionDeadline(ctx, anchorBatchID, 365*24*time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	retryDeadline, err := repository2.batchRetentionDeadline(ctx, anchorBatchID, 365*24*time.Hour, true)
	if err != nil || !anchorDeadline.Equal(retryDeadline) {
		t.Fatalf("anchor receipt deadline did not converge: first=%s retry=%s err=%v", anchorDeadline, retryDeadline, err)
	}
	entered, err := repository1.EnforceSealingHealth(ctx, time.Hour, errors.New("temporary WORM HTTP 503"))
	if err != nil || entered {
		t.Fatalf("temporary retention outage entered safe mode before maximum lag: entered=%t err=%v", entered, err)
	}
	entered, err = repository1.EnforceSealingHealth(ctx, time.Hour, errors.Join(ErrIdempotencyConflict, errors.New("retention proof mismatch")))
	if err != nil || !entered {
		t.Fatalf("authenticated retention mismatch did not enter safe mode: entered=%t err=%v", entered, err)
	}
}

func TestConcurrentReplicaSealersSerializeCompleteStateMachine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool1 := financialTestPool(t)
	pool2, err := pgxpool.New(ctx, os.Getenv("ATOS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	repository1, _ := NewRepository(pool1, "gw-sealer-"+suffix, "net-sealer-"+suffix)
	repository2, _ := NewRepository(pool2, "gw-sealer-"+suffix, "net-sealer-"+suffix)
	ledger := newTestLedger()
	adapter, _ := NewAdapter(repository1, ledger)
	request := requestFor("sealer-genesis-"+suffix, EventAccountGenesis, GatewayCreditIssuance, PrincipalAvailable, "_", "principal-"+suffix)
	request.AllowOverdraft = true
	event, err := adapter.ProvisionAccount(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	transaction, found, err := ledger.Lookup(ctx, event.LedgerReference)
	if err != nil || !found {
		t.Fatalf("finalized transaction missing: found=%t err=%v", found, err)
	}
	genesis := strings.Repeat("0", 64)
	row := LedgerChainRow{Transaction: transaction, Amount: event.AtomicAmount, ChainVersion: blnkChainVersionCBORV3,
		ChainSequence: 1, ChainPreviousHash: genesis}
	row.ChainHash, err = ledgerChainHash(genesis, row)
	if err != nil {
		t.Fatal(err)
	}
	chainLedger := &staticChainLedger{testLedger: ledger, evidence: LedgerChainEvidence{
		State: LedgerChainState{ChainKey: "global", FirstSequence: 1, LastSequence: 1, PreviousHash: genesis,
			HeadHash: row.ChainHash, GenesisHash: genesis}, Transactions: []LedgerChainRow{row}}}
	batch, err := repository1.CreateBatch(ctx, 100, chainLedger)
	if err != nil {
		t.Fatal(err)
	}
	signer := newCountingSigner(t)
	if _, err := repository1.SignBatch(ctx, batch, signer, "kms-sealer-test", "ed25519", signer.public); err != nil {
		t.Fatal(err)
	}
	retainer := newBlockingRetainer()
	publisher := &countingAnchorPublisher{}
	type sealResult struct {
		batch Batch
		err   error
	}
	results := make(chan sealResult, 2)
	seal := func(repository *Repository) {
		sealed, sealErr := repository.SealNext(ctx, chainLedger, signer, "kms-sealer-test", "ed25519", signer.public, retainer, publisher, 100)
		results <- sealResult{batch: sealed, err: sealErr}
	}
	go seal(repository1)
	select {
	case <-retainer.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go seal(repository2)
	for pool2.Stat().AcquiredConns() == 0 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(retainer.release)
	for range 2 {
		result := <-results
		if result.err != nil && !errors.Is(result.err, pgx.ErrNoRows) {
			t.Fatalf("normal replica sealing contention failed: batch=%s err=%v", result.batch.Manifest.BatchID, result.err)
		}
		if errors.Is(result.err, ErrIdempotencyConflict) {
			t.Fatalf("normal replica sealing contention became an integrity conflict: %v", result.err)
		}
	}
	var state string
	if err := pool1.QueryRow(ctx, `SELECT state FROM financial_batches WHERE batch_id=$1`, batch.Manifest.BatchID).Scan(&state); err != nil || state != "anchored" {
		t.Fatalf("sealed batch state=%q err=%v", state, err)
	}
	retainer.mu.Lock()
	putCalls := retainer.putCalls
	retainer.mu.Unlock()
	publisher.mu.Lock()
	publishCalls := publisher.count
	publisher.mu.Unlock()
	if signer.calls.Load() != 1 || putCalls != 2 || publishCalls != 1 {
		t.Fatalf("external effects signer=%d WORM puts=%d TOS publishes=%d", signer.calls.Load(), putCalls, publishCalls)
	}
	safeMode, reason, err := repository1.SafeMode(ctx)
	if err != nil || safeMode {
		t.Fatalf("normal replica sealing contention entered safe mode: safe_mode=%t reason=%q err=%v", safeMode, reason, err)
	}
}

func TestSealerRejectsSingleConnectionPoolWithoutDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	repository, _ := NewRepository(pool, "gw-single-sealer", "net-single-sealer")
	_, err = repository.SealNext(ctx, (*staticChainLedger)(nil), nil, "", "", "", nil, nil, 1)
	if err == nil || !strings.Contains(err.Error(), "at least two PostgreSQL pool connections") {
		t.Fatalf("single-connection sealer error=%v", err)
	}
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
