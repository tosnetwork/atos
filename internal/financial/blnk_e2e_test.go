package financial

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRealBlnkTwoReplicaLifecycleAndLostResponse(t *testing.T) {
	baseURL := os.Getenv("ATOS_TEST_BLNK_URL")
	if baseURL == "" {
		t.Skip("ATOS_TEST_BLNK_URL is required")
	}
	ctx := context.Background()
	pool1 := financialTestPool(t)
	pool2, err := pgxpool.New(ctx, os.Getenv("ATOS_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	gateway, network := "e2e-gw-"+suffix, "e2e-net-"+suffix
	principal, provider, job := "p-"+suffix, "v-"+suffix, "j-"+suffix
	repo1, _ := NewRepository(pool1, gateway, network)
	repo2, _ := NewRepository(pool2, gateway, network)
	client, err := NewBlnkClient(BlnkConfig{BaseURL: baseURL, Timeout: 10 * time.Second, GenesisIssuanceLimit: "101.00"})
	if err != nil {
		t.Fatal(err)
	}
	a1, _ := NewAdapter(repo1, client)
	a2, _ := NewAdapter(repo2, client)
	ids := Identities{PrincipalID: principal, ProviderID: provider, JobID: job, QuoteID: "q-" + suffix, CapabilityID: "c-" + suffix, CapabilityVersion: "1", BillingSnapshotID: "b-" + suffix, ExecutionReceiptID: "r-" + suffix, SettlementID: "s-" + suffix, ProviderEarningID: "e-" + suffix, DisputeID: "d-" + suffix, PayoutID: "po-" + suffix}
	call := func(adapter *Adapter, method string, request TransferRequest) {
		t.Helper()
		var err error
		switch method {
		case "genesis":
			_, err = adapter.ProvisionAccount(ctx, request)
		case "reserve":
			_, err = adapter.Reserve(ctx, request)
		case "fund":
			_, err = adapter.FundEscrow(ctx, request)
		case "settle":
			_, err = adapter.Settle(ctx, request)
		case "hold":
			_, err = adapter.HoldDispute(ctx, request)
		case "release":
			_, err = adapter.ReleaseDispute(ctx, request)
		case "gateway-refund-fund":
			_, err = adapter.FundGatewayRefund(ctx, request)
		case "gateway-refund-pay":
			_, err = adapter.PayGatewayRefund(ctx, request)
		case "payout":
			_, err = adapter.BeginPayout(ctx, request)
		case "paid":
			_, err = adapter.CompletePayout(ctx, request)
		}
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
	}
	base := TransferRequest{Identities: ids, Asset: "USD", Decimals: 2}
	genesis := base
	genesis.EventType = EventAccountGenesis
	genesis.IdempotencyIdentity = "principal:" + principal + ":genesis:v1"
	genesis.AtomicAmount = "10000"
	genesis.SourceCode = GatewayCreditIssuance
	genesis.SourceOwnerID = "_"
	genesis.DestinationCode = PrincipalAvailable
	genesis.DestinationOwnerID = principal
	genesis.AllowOverdraft = true
	call(a1, "genesis", genesis)
	reserve := base
	reserve.EventType = EventReserve
	reserve.IdempotencyIdentity = "job:" + job + ":reserve:v1"
	reserve.AtomicAmount = "1000"
	reserve.SourceCode = PrincipalAvailable
	reserve.SourceOwnerID = principal
	reserve.DestinationCode = PrincipalReserved
	reserve.DestinationOwnerID = principal
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := a1
			if i%2 == 1 {
				a = a2
			}
			_, err := a.Reserve(ctx, reserve)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	fund := base
	fund.EventType = EventEscrowFund
	fund.IdempotencyIdentity = "job:" + job + ":escrow-fund:v1"
	fund.AtomicAmount = "1000"
	fund.SourceCode = PrincipalReserved
	fund.SourceOwnerID = principal
	fund.DestinationCode = ManagedEscrow
	fund.DestinationOwnerID = job
	call(a2, "fund", fund)
	providerLeg := base
	providerLeg.EventType = EventSettlementProvider
	providerLeg.IdempotencyIdentity = "settlement:s-" + suffix + ":provider:v1"
	providerLeg.AtomicAmount = "700"
	providerLeg.SourceCode = ManagedEscrow
	providerLeg.SourceOwnerID = job
	providerLeg.DestinationCode = ProviderPayable
	providerLeg.DestinationOwnerID = provider
	call(a1, "settle", providerLeg)
	fee := base
	fee.EventType = EventSettlementFee
	fee.IdempotencyIdentity = "settlement:s-" + suffix + ":fee:v1"
	fee.AtomicAmount = "100"
	fee.SourceCode = ManagedEscrow
	fee.SourceOwnerID = job
	fee.DestinationCode = GatewayFeeRevenue
	fee.DestinationOwnerID = "_"
	call(a2, "settle", fee)
	gatewayRefundFund := base
	gatewayRefundFund.EventType = EventGatewayRefundFund
	gatewayRefundFund.IdempotencyIdentity = "refund:d-" + suffix + ":gateway-fee-fund:v1"
	gatewayRefundFund.AtomicAmount = "100"
	gatewayRefundFund.SourceCode = GatewayFeeRevenue
	gatewayRefundFund.SourceOwnerID = "_"
	gatewayRefundFund.DestinationCode = GatewayRefundLiability
	gatewayRefundFund.DestinationOwnerID = "_"
	call(a1, "gateway-refund-fund", gatewayRefundFund)
	gatewayRefundPay := base
	gatewayRefundPay.EventType = EventGatewayRefundPay
	gatewayRefundPay.IdempotencyIdentity = "refund:d-" + suffix + ":gateway-fee-pay:v1"
	gatewayRefundPay.AtomicAmount = "100"
	gatewayRefundPay.SourceCode = GatewayRefundLiability
	gatewayRefundPay.SourceOwnerID = "_"
	gatewayRefundPay.DestinationCode = PrincipalAvailable
	gatewayRefundPay.DestinationOwnerID = principal
	call(a2, "gateway-refund-pay", gatewayRefundPay)
	refund := base
	refund.EventType = EventSettlementRefund
	refund.IdempotencyIdentity = "settlement:s-" + suffix + ":refund:v1"
	refund.AtomicAmount = "200"
	refund.SourceCode = ManagedEscrow
	refund.SourceOwnerID = job
	refund.DestinationCode = PrincipalAvailable
	refund.DestinationOwnerID = principal
	call(a1, "settle", refund)
	hold := base
	hold.EventType = EventDisputeHold
	hold.IdempotencyIdentity = "dispute:d-" + suffix + ":hold:v1"
	hold.AtomicAmount = "700"
	hold.SourceCode = ProviderPayable
	hold.SourceOwnerID = provider
	hold.DestinationCode = ProviderDisputed
	hold.DestinationOwnerID = provider
	call(a1, "hold", hold)
	release := base
	release.EventType = EventDisputeRelease
	release.IdempotencyIdentity = "dispute:d-" + suffix + ":release:v1"
	release.AtomicAmount = "700"
	release.SourceCode = ProviderDisputed
	release.SourceOwnerID = provider
	release.DestinationCode = ProviderPayable
	release.DestinationOwnerID = provider
	call(a2, "release", release)
	payoutID := "po-" + suffix
	payout := base
	payout.EventType = EventPayoutIntent
	payout.IdempotencyIdentity = "payout:" + payoutID + ":intent:v1"
	payout.AtomicAmount = "700"
	payout.SourceCode = ProviderPayable
	payout.SourceOwnerID = provider
	payout.DestinationCode = PayoutClearing
	payout.DestinationOwnerID = payoutID
	call(a1, "payout", payout)
	paid := base
	paid.EventType = EventPayoutSuccess
	paid.IdempotencyIdentity = "payout:" + payoutID + ":success:v1"
	paid.AtomicAmount = "700"
	paid.SourceCode = PayoutClearing
	paid.SourceOwnerID = payoutID
	paid.DestinationCode = PayoutDisbursed
	paid.DestinationOwnerID = "_"
	call(a2, "paid", paid)
	available, err := a1.Balance(ctx, PrincipalAvailable, principal, "USD", 2)
	if err != nil || available.AtomicAmount != "9300" {
		t.Fatalf("available=%+v err=%v", available, err)
	}
	changed := reserve
	changed.AtomicAmount = "999"
	if _, err := a1.Reserve(ctx, changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed semantic retry=%v", err)
	}

	// Forward one POST to the real Blnk service, then close the caller socket
	// before returning the response. Adapter recovery must resolve by reference.
	upstream, _ := url.Parse(baseURL)
	var lose atomic.Bool
	lose.Store(true)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := *upstream
		target.Path = r.URL.Path
		target.RawQuery = r.URL.RawQuery
		out := r.Clone(r.Context())
		out.URL = &target
		out.RequestURI = ""
		response, err := http.DefaultClient.Do(out)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer response.Body.Close()
		if r.Method == http.MethodPost && lose.CompareAndSwap(true, false) {
			_, _ = io.Copy(io.Discard, response.Body)
			if h, ok := w.(http.Hijacker); ok {
				conn, _, _ := h.Hijack()
				_ = conn.Close()
				return
			}
		}
		for k, v := range response.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	defer proxy.Close()
	lostClient, _ := NewBlnkClient(BlnkConfig{BaseURL: proxy.URL, Timeout: 10 * time.Second, GenesisIssuanceLimit: "101.00"})
	lostAdapter, _ := NewAdapter(repo1, lostClient)
	secondPrincipal := "p-lost-" + suffix
	lost := base
	lost.EventType = EventAccountGenesis
	lost.IdempotencyIdentity = "principal:" + secondPrincipal + ":genesis:v1"
	lost.Identities.PrincipalID = secondPrincipal
	lost.AtomicAmount = "50"
	lost.SourceCode = GatewayCreditIssuance
	lost.SourceOwnerID = "_"
	lost.DestinationCode = PrincipalAvailable
	lost.DestinationOwnerID = secondPrincipal
	lost.AllowOverdraft = true
	if _, err := lostAdapter.ProvisionAccount(ctx, lost); err != nil {
		t.Fatalf("real Blnk lost response did not converge: %v", err)
	}
	if result, err := a1.Reconcile(ctx, 100); err != nil || result.SafeMode || result.Mismatches != 0 {
		t.Fatalf("real Blnk reconciliation result=%+v err=%v", result, err)
	}
	overLimit := lost
	overLimit.IdempotencyIdentity = "principal:p-over-limit-" + suffix + ":genesis:v1"
	overLimit.Identities.PrincipalID = "p-over-limit-" + suffix
	overLimit.DestinationOwnerID = overLimit.Identities.PrincipalID
	overLimit.AtomicAmount = "51"
	if _, err := a1.ProvisionAccount(ctx, overLimit); err == nil {
		t.Fatal("Blnk accepted genesis beyond the configured aggregate issuance limit")
	}
}
