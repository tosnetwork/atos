package toprotocol

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tosnetwork/atos/internal/financial"
	"github.com/tosnetwork/atos/internal/store/memory"
	"github.com/tosnetwork/atos/migrations"
	"github.com/tosnetwork/tos-protocol/pkg/atosrpc"
)

type finalizedFinancialAuthority struct{ network string }

func (a *finalizedFinancialAuthority) Network() string { return a.network }
func (a *finalizedFinancialAuthority) Supports(mode atosrpc.TrustMode) bool {
	return mode == atosrpc.TrustModeManaged
}
func (a *finalizedFinancialAuthority) CheckReady(context.Context) error { return nil }
func (a *finalizedFinancialAuthority) Close() error                     { return nil }
func (a *finalizedFinancialAuthority) Commit(_ context.Context, kind, id, digest string) (atosrpc.NetworkReference, error) {
	hash := sha256.Sum256([]byte(kind + "\x00" + id + "\x00" + digest))
	return atosrpc.NetworkReference{Network: a.network, Reference: "tos:tx:v1:" + hex.EncodeToString(hash[:]), Finalized: true, FinalizedCheckpoint: 91}, nil
}
func (a *finalizedFinancialAuthority) ResolveCommitment(_ context.Context, _, _, _ string, reference *atosrpc.NetworkReference) (*atosrpc.NetworkReference, error) {
	return &atosrpc.NetworkReference{Network: reference.Network, Reference: reference.Reference, Finalized: reference.Finalized, FinalizedCheckpoint: reference.FinalizedCheckpoint}, nil
}

type retainedObject struct {
	body    []byte
	digest  string
	version string
}

func TestFinancialBatchExternalSignerWORMRPCAndVerifier(t *testing.T) {
	dsn, blnkURL := os.Getenv("ATOS_TEST_DATABASE_URL"), os.Getenv("ATOS_TEST_BLNK_URL")
	if dsn == "" || blnkURL == "" {
		t.Skip("ATOS_TEST_DATABASE_URL and ATOS_TEST_BLNK_URL are required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE financial_events,financial_projections,financial_integrity_incidents,financial_batches,financial_reconciler_leases`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_chain_state SET next_sequence=1,last_commitment=$1,next_batch_sequence=1,last_batch_id='',last_batch_root=$1,last_anchor_id=''`, financial.GenesisDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_integrity_state SET safe_mode=FALSE,reason='',incident_id='',entered_at=NULL`); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprint(time.Now().UnixNano())
	gateway, network := "gateway-"+suffix, "tos-dev-financial"
	repository, err := financial.NewRepository(pool, gateway, network)
	if err != nil {
		t.Fatal(err)
	}
	blnk, err := financial.NewBlnkClient(financial.BlnkConfig{BaseURL: blnkURL, Timeout: 10 * time.Second, GenesisIssuanceLimit: "1000000.00"})
	if err != nil {
		t.Fatal(err)
	}
	adapter, _ := financial.NewAdapter(repository, blnk)
	principal := "principal-" + suffix
	_, err = adapter.ProvisionAccount(ctx, financial.TransferRequest{
		EventType: financial.EventAccountGenesis, IdempotencyIdentity: "principal:" + principal + ":genesis:v1",
		Identities: financial.Identities{PrincipalID: principal}, Asset: "USD", Decimals: 2, AtomicAmount: "1000",
		SourceCode: financial.GatewayCreditIssuance, SourceOwnerID: "_", DestinationCode: financial.PrincipalAvailable,
		DestinationOwnerID: principal, AllowOverdraft: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signerToken := strings.Repeat("signer-service-token-", 2)
	signerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var input financial.SignRequest
		if request.Method != http.MethodPost || request.Header.Get("Authorization") != "Bearer "+signerToken || json.NewDecoder(io.LimitReader(request.Body, 1<<20)).Decode(&input) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		digest, err := financial.DigestBytes(input.Digest)
		if err != nil || input.KeyID != "kms-key-2026-08" || input.Algorithm != "ed25519" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(writer).Encode(financial.SignResponse{KeyID: input.KeyID, Algorithm: input.Algorithm,
			Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, digest)),
			PublicKey: base64.StdEncoding.EncodeToString(publicKey), SignedUnixMillis: 1786406400000})
	}))
	defer signerServer.Close()
	signer, _ := financial.NewHTTPSigner(signerServer.URL, "kms-key-2026-08", "ed25519", signerToken, 10*time.Second)

	var objectMu sync.Mutex
	objects := make(map[string]retainedObject)
	loseFirstResponse := true
	retentionHMACKey := strings.Repeat("retention-auth-key-", 2)
	wormServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		objectMu.Lock()
		defer objectMu.Unlock()
		timestamp := request.Header.Get("X-ATOS-Retention-Timestamp")
		timestampValue, timestampErr := strconv.ParseInt(timestamp, 10, 64)
		if timestampErr != nil || timestampValue < time.Now().Unix()-300 || timestampValue > time.Now().Unix()+300 {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		digest := request.Header.Get("X-Content-SHA256")
		target := request.URL.EscapedPath()
		if request.URL.RawQuery != "" {
			target += "?" + request.URL.RawQuery
		}
		message := timestamp + "\n" + request.Method + "\n" + target + "\n" + digest
		mac := hmac.New(sha256.New, []byte(retentionHMACKey))
		_, _ = mac.Write([]byte(message))
		expectedAuth := "hmac-sha256=" + hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(request.Header.Get("X-ATOS-Retention-Signature")), []byte(expectedAuth)) {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		key := strings.TrimPrefix(request.URL.Path, "/")
		switch request.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(io.LimitReader(request.Body, 4<<20))
			bodyHash := sha256.Sum256(body)
			if digest != "sha256:"+hex.EncodeToString(bodyHash[:]) {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if existing, found := objects[key]; found {
				if existing.digest != digest {
					writer.WriteHeader(http.StatusConflict)
					return
				}
				writer.Header().Set("X-Object-Version-ID", existing.version)
				writer.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			objects[key] = retainedObject{body: body, digest: digest, version: "locked-v1"}
			if loseFirstResponse {
				loseFirstResponse = false
				if hijacker, ok := writer.(http.Hijacker); ok {
					connection, _, _ := hijacker.Hijack()
					_ = connection.Close()
					return
				}
			}
			writer.Header().Set("X-Object-Version-ID", "locked-v1")
			writer.WriteHeader(http.StatusCreated)
		case http.MethodHead:
			object, found := objects[key]
			if !found {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.Header().Set("X-Content-SHA256", object.digest)
			writer.Header().Set("X-Object-Version-ID", object.version)
			writer.Header().Set("X-Object-Lock-Mode", "COMPLIANCE")
			writer.Header().Set("X-Object-Retain-Until", time.Now().UTC().Add(24*time.Hour).Format(time.RFC3339))
			writer.WriteHeader(http.StatusOK)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer wormServer.Close()
	retainer, _ := financial.NewHTTPRetainer(wormServer.URL, retentionHMACKey, 10*time.Second)

	router, _ := atosrpc.NewStaticRouter(nil)
	protocolServer, err := atosrpc.Open(atosrpc.Config{StatePath: t.TempDir() + "/protocol.db", BearerToken: "phase7a-token", Authority: &finalizedFinancialAuthority{network: network}, Router: router})
	if err != nil {
		t.Fatal(err)
	}
	defer protocolServer.Close()
	rpcServer := httptest.NewServer(protocolServer.Handler())
	defer rpcServer.Close()
	protocolClient, err := New(Config{BaseURL: rpcServer.URL, BearerToken: "phase7a-token", Insecure: true, Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}

	trustedPublicKey := base64.StdEncoding.EncodeToString(publicKey)
	var batch financial.Batch
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		batch, err = repository.SealNext(ctx, blnk, signer, "kms-key-2026-08", "ed25519", trustedPublicKey, retainer, protocolClient, 100)
		if err == nil || time.Now().After(deadline) || !errors.Is(err, financial.ErrLedgerUncertain) {
			break
		}
	}
	if err != nil || batch.State != "anchored" {
		t.Fatalf("seal batch=%+v err=%v", batch, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_batches SET state='created' WHERE batch_id=$1`, batch.Manifest.BatchID); err == nil {
		t.Fatal("anchored batch was downgraded to an unsealed state")
	}
	if _, err := pool.Exec(ctx, `UPDATE financial_batches SET anchor_id='replacement' WHERE batch_id=$1`, batch.Manifest.BatchID); err == nil {
		t.Fatal("anchored batch identity mutation succeeded")
	}
	objectMu.Lock()
	var evidenceBytes, anchorBytes []byte
	for key, object := range objects {
		if strings.Contains(key, "/anchors/") {
			anchorBytes = append([]byte(nil), object.body...)
		} else {
			evidenceBytes = append([]byte(nil), object.body...)
		}
	}
	objectMu.Unlock()
	if len(evidenceBytes) == 0 || len(anchorBytes) == 0 {
		t.Fatalf("retained objects missing: %d", len(objects))
	}
	bundle, err := financial.DecodeEvidenceBundle(evidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	var receipt financial.AnchorReceipt
	if err := json.Unmarshal(anchorBytes, &receipt); err != nil {
		t.Fatal(err)
	}
	trusted := map[string]string{"kms-key-2026-08": trustedPublicKey}
	verifyOptions := financial.VerifyOptions{GatewayID: gateway, NetworkID: network, TrustedPublicKeys: trusted, Resolver: protocolClient, RetentionResolver: retainer, RetainedVersionID: "locked-v1"}
	if err := financial.VerifyEvidence(ctx, bundle, receipt, verifyOptions); err != nil {
		t.Fatal(err)
	}
	tampered := bundle
	tampered.Commitments = append([]financial.Commitment(nil), bundle.Commitments...)
	tampered.Commitments[0].AtomicAmount = "999"
	if err := financial.VerifyEvidence(ctx, tampered, receipt, verifyOptions); err == nil {
		t.Fatal("independent verifier accepted a changed sealed amount")
	}
}
