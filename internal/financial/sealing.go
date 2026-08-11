package financial

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tosnetwork/tos-protocol/pkg/codec"
)

type SignRequest struct {
	StableIdentity string `json:"stable_identity"`
	KeyID          string `json:"key_id"`
	Algorithm      string `json:"algorithm"`
	Digest         string `json:"digest"`
}

type SignResponse struct {
	KeyID            string `json:"key_id"`
	Algorithm        string `json:"algorithm"`
	Signature        string `json:"signature"`
	PublicKey        string `json:"public_key"`
	SignedUnixMillis int64  `json:"signed_unix_millis"`
}

type ExternalSigner interface {
	Sign(context.Context, SignRequest) (SignResponse, error)
}

type SignatureEnvelope struct {
	Version          string `json:"version"`
	BatchID          string `json:"batch_id"`
	ManifestDigest   string `json:"manifest_digest"`
	SigningDigest    string `json:"signing_digest"`
	GatewayID        string `json:"gateway_id"`
	NetworkID        string `json:"network_id"`
	SigningKeyID     string `json:"signing_key_id"`
	SigningAlgorithm string `json:"signing_algorithm"`
	Signature        string `json:"signature"`
	PublicKey        string `json:"public_key"`
	SignedUnixMillis int64  `json:"signed_unix_millis"`
}

func signingDigest(manifest BatchManifest) (string, error) {
	return codec.Digest(BatchSignatureDomain, manifest)
}

func VerifySignature(envelope SignatureEnvelope) error {
	rawDigest, err := DigestBytes(envelope.SigningDigest)
	if err != nil {
		return err
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(envelope.Signature)
	if err != nil {
		return errors.New("financial: invalid signature encoding")
	}
	publicKey, err := base64.StdEncoding.Strict().DecodeString(envelope.PublicKey)
	if err != nil {
		return errors.New("financial: invalid public key encoding")
	}
	switch envelope.SigningAlgorithm {
	case "ed25519":
		if len(publicKey) != ed25519.PublicKeySize || !ed25519.Verify(ed25519.PublicKey(publicKey), rawDigest, signature) {
			return errors.New("financial: invalid Ed25519 batch signature")
		}
	case "ecdsa_p256_sha256":
		parsed, err := x509.ParsePKIXPublicKey(publicKey)
		if err != nil {
			return errors.New("financial: invalid ECDSA public key")
		}
		key, ok := parsed.(*ecdsa.PublicKey)
		if !ok || key.Curve != elliptic.P256() || !ecdsa.VerifyASN1(key, rawDigest, signature) {
			return errors.New("financial: invalid ECDSA batch signature")
		}
	default:
		return errors.New("financial: unsupported signing algorithm")
	}
	return nil
}

type HTTPSigner struct {
	endpoint  string
	keyID     string
	algorithm string
	token     string
	client    *http.Client
}

func NewHTTPSigner(endpoint, keyID, algorithm, token string, timeout time.Duration) (*HTTPSigner, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || keyID == "" || len(token) < 32 || (algorithm != "ed25519" && algorithm != "ecdsa_p256_sha256") || timeout <= 0 {
		return nil, errors.New("financial: invalid external signer configuration")
	}
	return &HTTPSigner{endpoint: endpoint, keyID: keyID, algorithm: algorithm, token: token, client: &http.Client{Timeout: timeout}}, nil
}

func (s *HTTPSigner) Sign(ctx context.Context, request SignRequest) (SignResponse, error) {
	request.KeyID, request.Algorithm = s.keyID, s.algorithm
	body, err := json.Marshal(request)
	if err != nil {
		return SignResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return SignResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.token)
	response, err := s.client.Do(req)
	if err != nil {
		return SignResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return SignResponse{}, fmt.Errorf("financial: signer returned HTTP %d", response.StatusCode)
	}
	var output SignResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&output); err != nil {
		return SignResponse{}, err
	}
	return output, nil
}

func (r *Repository) signBatchWith(ctx context.Context, db repositoryDB, batch Batch, signer ExternalSigner, keyID, algorithm, trustedPublicKey string) (SignatureEnvelope, error) {
	if signer == nil {
		return SignatureEnvelope{}, errors.New("financial: external signer is required")
	}
	if _, err := base64.StdEncoding.Strict().DecodeString(trustedPublicKey); err != nil || trustedPublicKey == "" {
		return SignatureEnvelope{}, errors.New("financial: trusted signer public key is required")
	}
	digest, err := signingDigest(batch.Manifest)
	if err != nil {
		return SignatureEnvelope{}, err
	}
	response, err := signer.Sign(ctx, SignRequest{StableIdentity: batch.Manifest.BatchID + ":" + batch.ManifestDigest, KeyID: keyID, Algorithm: algorithm, Digest: digest})
	if err != nil {
		return SignatureEnvelope{}, err
	}
	if response.KeyID != keyID || response.Algorithm != algorithm || subtle.ConstantTimeCompare([]byte(response.PublicKey), []byte(trustedPublicKey)) != 1 {
		return SignatureEnvelope{}, errors.Join(ErrIdempotencyConflict, errors.New("financial: signer substituted key or algorithm"))
	}
	envelope := SignatureEnvelope{"atos_financial_batch_signature_v2", batch.Manifest.BatchID, batch.ManifestDigest, digest,
		batch.Manifest.GatewayID, batch.Manifest.NetworkID, response.KeyID, response.Algorithm,
		response.Signature, response.PublicKey, response.SignedUnixMillis}
	if err := VerifySignature(envelope); err != nil {
		return SignatureEnvelope{}, errors.Join(ErrIdempotencyConflict, err)
	}
	raw, _ := json.Marshal(envelope)
	result, err := db.Exec(ctx, `UPDATE financial_batches SET signing_key_id=$2,signature_envelope=$3,state='signed',updated_at=now()
 WHERE batch_id=$1 AND state IN ('created','signed') AND (signature_envelope IS NULL OR signature_envelope=$3)`, batch.Manifest.BatchID, keyID, raw)
	if err != nil {
		return SignatureEnvelope{}, err
	}
	if result.RowsAffected() != 1 {
		return SignatureEnvelope{}, ErrIdempotencyConflict
	}
	return envelope, nil
}

func (r *Repository) EnforceSealingHealth(ctx context.Context, maximumLag time.Duration, cause error) (bool, error) {
	if maximumLag <= 0 || maximumLag > 24*time.Hour {
		return false, errors.New("financial: invalid maximum anchor lag")
	}
	var lagSeconds float64
	err := r.pool.QueryRow(ctx, `SELECT COALESCE(EXTRACT(EPOCH FROM now()-min(created_at)),0)
 FROM (SELECT created_at FROM financial_batches WHERE state<>'anchored'
       UNION ALL SELECT created_at FROM financial_events WHERE state='finalized' AND batch_id='') pending`).Scan(&lagSeconds)
	if err != nil {
		return false, err
	}
	if !errors.Is(cause, ErrIdempotencyConflict) && lagSeconds < maximumLag.Seconds() {
		return false, nil
	}
	classification := "integrity_sealing_mismatch"
	if !errors.Is(cause, ErrIdempotencyConflict) {
		classification = "integrity_anchor_lag_exceeded"
	}
	_, err = r.EnterSafeMode(ctx, classification, map[string]any{"maximum_lag_seconds": maximumLag.Seconds()}, map[string]any{"pending_lag_seconds": lagSeconds})
	return true, err
}

type EvidenceBundle struct {
	Version        string              `json:"version"`
	Manifest       BatchManifest       `json:"manifest"`
	ManifestCBOR   string              `json:"manifest_cbor"`
	Commitments    []Commitment        `json:"commitments"`
	LedgerEvidence LedgerChainEvidence `json:"ledger_evidence"`
	Signature      SignatureEnvelope   `json:"signature"`
}

type Retainer interface {
	PutIfAbsent(context.Context, string, []byte, string) (string, error)
	ResolveRetention(context.Context, string, string, string) (RetentionProof, error)
	MinimumRetention() time.Duration
}

type HTTPRetainer struct {
	endpoint         *url.URL
	client           *http.Client
	hmacKey          []byte
	minimumRetention time.Duration
}

func NewHTTPRetainer(endpoint, hmacKey string, timeout, minimumRetention time.Duration) (*HTTPRetainer, error) {
	if minimumRetention <= 0 {
		return nil, errors.New("financial: invalid WORM endpoint")
	}
	return newHTTPRetainer(endpoint, hmacKey, timeout, minimumRetention)
}

// NewHTTPRetentionResolver constructs a read-only exact-version resolver. The
// verifier supplies any remaining-retention policy separately from transport.
func NewHTTPRetentionResolver(endpoint, hmacKey string, timeout time.Duration) (*HTTPRetainer, error) {
	return newHTTPRetainer(endpoint, hmacKey, timeout, 0)
}

func newHTTPRetainer(endpoint, hmacKey string, timeout, minimumRetention time.Duration) (*HTTPRetainer, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || timeout <= 0 || len(hmacKey) < 32 {
		return nil, errors.New("financial: invalid WORM endpoint")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return &HTTPRetainer{parsed, &http.Client{Timeout: timeout}, []byte(hmacKey), minimumRetention}, nil
}

func (r *HTTPRetainer) MinimumRetention() time.Duration { return r.minimumRetention }

func (r *HTTPRetainer) authenticate(request *http.Request, digest string) {
	timestamp := fmt.Sprint(time.Now().UTC().Unix())
	target := request.URL.EscapedPath()
	if request.URL.RawQuery != "" {
		target += "?" + request.URL.RawQuery
	}
	message := timestamp + "\n" + request.Method + "\n" + target + "\n" + digest
	mac := hmac.New(sha256.New, r.hmacKey)
	_, _ = mac.Write([]byte(message))
	request.Header.Set("X-Content-SHA256", digest)
	request.Header.Set("X-ATOS-Retention-Timestamp", timestamp)
	request.Header.Set("X-ATOS-Retention-Signature", "hmac-sha256="+hex.EncodeToString(mac.Sum(nil)))
}

type RetentionProof struct {
	ObjectKey   string
	VersionID   string
	Digest      string
	LockMode    string
	RetainUntil time.Time
}

type RetentionResolver interface {
	ResolveRetention(context.Context, string, string, string) (RetentionProof, error)
}

func (r *HTTPRetainer) ResolveRetention(ctx context.Context, key, versionID, digest string) (RetentionProof, error) {
	if key == "" || versionID == "" || digest == "" {
		return RetentionProof{}, errors.New("financial: incomplete immutable retention identity")
	}
	endpoint := *r.endpoint
	endpoint.Path += "/" + strings.TrimLeft(key, "/")
	query := endpoint.Query()
	query.Set("version_id", versionID)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint.String(), nil)
	if err != nil {
		return RetentionProof{}, err
	}
	r.authenticate(request, digest)
	response, err := r.client.Do(request)
	if err != nil {
		return RetentionProof{}, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	retainUntil, timeErr := time.Parse(time.RFC3339, response.Header.Get("X-Object-Retain-Until"))
	proof := RetentionProof{ObjectKey: key, VersionID: response.Header.Get("X-Object-Version-ID"),
		Digest: response.Header.Get("X-Content-SHA256"), LockMode: response.Header.Get("X-Object-Lock-Mode"), RetainUntil: retainUntil}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusNotFound {
			return RetentionProof{}, errors.Join(ErrIdempotencyConflict, errors.New("financial: exact retained object version is missing"))
		}
		return RetentionProof{}, fmt.Errorf("financial: WORM retention lookup returned HTTP %d", response.StatusCode)
	}
	if timeErr != nil || proof.VersionID != versionID || proof.Digest != digest || proof.LockMode != "COMPLIANCE" {
		return RetentionProof{}, errors.Join(ErrIdempotencyConflict, errors.New("financial: authenticated retention proof mismatch"))
	}
	return proof, nil
}

func (r *HTTPRetainer) resolve(ctx context.Context, endpoint string, digest string) (string, error) {
	head, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		return "", err
	}
	r.authenticate(head, digest)
	resolved, err := r.client.Do(head)
	if err != nil {
		return "", err
	}
	defer resolved.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resolved.Body, 4096))
	version := strings.TrimSpace(resolved.Header.Get("X-Object-Version-ID"))
	if resolved.StatusCode != http.StatusOK {
		return "", fmt.Errorf("financial: WORM recovery lookup returned HTTP %d", resolved.StatusCode)
	}
	if resolved.Header.Get("X-Content-SHA256") != digest || version == "" {
		return "", errors.Join(ErrIdempotencyConflict, errors.New("financial: authenticated WORM recovery proof mismatch"))
	}
	return version, nil
}

func (r *HTTPRetainer) PutIfAbsent(ctx context.Context, key string, body []byte, digest string) (string, error) {
	endpoint := *r.endpoint
	endpoint.Path += "/" + strings.TrimLeft(key, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("If-None-Match", "*")
	req.Header.Set("Content-Type", "application/json")
	r.authenticate(req, digest)
	response, err := r.client.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		closeErr := response.Body.Close()
		accepted := response.StatusCode >= 200 && response.StatusCode < 300
		if !accepted && response.StatusCode != http.StatusConflict && response.StatusCode != http.StatusPreconditionFailed {
			return "", fmt.Errorf("financial: WORM store returned HTTP %d", response.StatusCode)
		}
		if closeErr != nil {
			err = closeErr
		}
	}
	// A successful PUT is not sufficient evidence of retention. Conflict,
	// success, and lost-response paths all authenticate a HEAD lookup and bind
	// the exact digest to a non-empty immutable object version.
	version, resolveErr := r.resolve(ctx, endpoint.String(), digest)
	if resolveErr == nil {
		return version, nil
	}
	return "", errors.Join(err, resolveErr)
}

type FileRetainer struct{ root string }

func NewFileRetainer(root string) (*FileRetainer, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("financial: retention root must be an absolute clean path")
	}
	return &FileRetainer{root}, nil
}
func (r *FileRetainer) PutIfAbsent(_ context.Context, key string, body []byte, digest string) (string, error) {
	clean := filepath.Clean(key)
	if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
		return "", errors.New("financial: invalid retention key")
	}
	path := filepath.Join(r.root, clean)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0400)
	if errors.Is(err, os.ErrExist) {
		existing, readErr := os.ReadFile(path)
		if readErr != nil {
			return "", readErr
		}
		hash := sha256.Sum256(existing)
		if "sha256:"+hex.EncodeToString(hash[:]) != digest {
			return "", ErrIdempotencyConflict
		}
		return digest, nil
	}
	if err != nil {
		return "", err
	}
	if _, err = file.Write(body); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	return digest, nil
}

// A local file can provide create-only test storage, but it cannot prove an
// independently administered Object Lock boundary and therefore cannot drive
// the production retained/anchored state machine.
func (r *FileRetainer) ResolveRetention(context.Context, string, string, string) (RetentionProof, error) {
	return RetentionProof{}, errors.New("financial: local file retention is not Object Lock evidence")
}

func (r *FileRetainer) MinimumRetention() time.Duration { return 0 }

func retainWithProof(ctx context.Context, retainer Retainer, key string, body []byte, digest string, requiredRetainUntil time.Time) (RetentionProof, error) {
	if retainer == nil || requiredRetainUntil.IsZero() {
		return RetentionProof{}, errors.New("financial: durable WORM retention deadline is required")
	}
	version, err := retainer.PutIfAbsent(ctx, key, body, digest)
	if err != nil {
		return RetentionProof{}, err
	}
	proof, err := retainer.ResolveRetention(ctx, key, version, digest)
	if err != nil {
		return RetentionProof{}, err
	}
	if proof.ObjectKey != key || proof.VersionID != version || proof.Digest != digest || proof.LockMode != "COMPLIANCE" ||
		!proof.RetainUntil.After(time.Now().UTC()) || proof.RetainUntil.Before(requiredRetainUntil) {
		return RetentionProof{}, errors.Join(ErrIdempotencyConflict,
			errors.New("financial: exact object version lacks required COMPLIANCE retention"))
	}
	return proof, nil
}

func (r *Repository) batchRetentionDeadlineWith(ctx context.Context, db repositoryDB, batchID string, minimum time.Duration, anchorReceipt bool) (time.Time, error) {
	if minimum <= 0 {
		return time.Time{}, errors.New("financial: positive minimum WORM retention is required")
	}
	candidate := time.Now().UTC().Truncate(time.Second).Add(minimum)
	query := `UPDATE financial_batches SET required_retain_until=COALESCE(required_retain_until,$2),updated_at=now()
 WHERE batch_id=$1 AND state IN ('signed','retained') RETURNING required_retain_until`
	if anchorReceipt {
		query = `UPDATE financial_batches SET anchor_required_retain_until=COALESCE(anchor_required_retain_until,$2),updated_at=now()
 WHERE batch_id=$1 AND state IN ('retained','anchored') RETURNING anchor_required_retain_until`
	}
	var deadline time.Time
	if err := db.QueryRow(ctx, query, batchID, candidate).Scan(&deadline); err != nil {
		return time.Time{}, err
	}
	return deadline.UTC(), nil
}

func (r *Repository) retainBatchWith(ctx context.Context, db repositoryDB, batch Batch, signature SignatureEnvelope, retainer Retainer) (string, string, error) {
	if retainer == nil {
		return "", "", errors.New("financial: retainer required")
	}
	bundle := EvidenceBundle{Version: "atos_financial_evidence_bundle_v2", Manifest: batch.Manifest,
		ManifestCBOR: base64.StdEncoding.EncodeToString(batch.ManifestCBOR), Commitments: batch.Commitments,
		LedgerEvidence: batch.LedgerEvidence, Signature: signature}
	body, err := json.Marshal(bundle)
	if err != nil {
		return "", "", err
	}
	hash := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	key := fmt.Sprintf("atos-financial/v1/%s/%s/%d-%s.json", batch.Manifest.GatewayID, batch.Manifest.NetworkID, batch.Manifest.BatchSequence, batch.Manifest.BatchID)
	deadline, err := r.batchRetentionDeadlineWith(ctx, db, batch.Manifest.BatchID, retainer.MinimumRetention(), false)
	if err != nil {
		return "", "", err
	}
	proof, err := retainWithProof(ctx, retainer, key, body, digest, deadline)
	if err != nil {
		return "", "", err
	}
	result, err := db.Exec(ctx, `UPDATE financial_batches SET retained_object_key=$2,retained_version_id=$3,state='retained',updated_at=now()
 WHERE batch_id=$1 AND state IN ('signed','retained') AND (retained_object_key='' OR retained_object_key=$2)`, batch.Manifest.BatchID, key, proof.VersionID)
	if err != nil {
		return "", "", err
	}
	if result.RowsAffected() != 1 {
		return "", "", ErrIdempotencyConflict
	}
	return key, proof.VersionID, nil
}

func (r *Repository) SealNext(ctx context.Context, ledger interface {
	ledgerClient
	ledgerChainReader
}, signer ExternalSigner, keyID, algorithm, trustedPublicKey string, retainer Retainer, publisher AnchorPublisher, limit int) (Batch, error) {
	lockConn, unlock, err := r.lockSealing(ctx)
	if err != nil {
		return Batch{}, err
	}
	defer unlock()

	batch, signature, err := r.pendingBatchWith(ctx, lockConn)
	if errors.Is(err, pgx.ErrNoRows) {
		batch, err = r.createBatchWith(ctx, lockConn, limit, ledger)
	}
	if err != nil {
		return Batch{}, err
	}
	if batch.State == "created" {
		envelope, err := r.signBatchWith(ctx, lockConn, batch, signer, keyID, algorithm, trustedPublicKey)
		if err != nil {
			return batch, err
		}
		signature = &envelope
		batch.State = "signed"
	}
	if signature == nil {
		return batch, errors.New("financial: sealed batch lacks signature")
	}
	if batch.State == "signed" {
		if _, _, err := r.retainBatchWith(ctx, lockConn, batch, *signature, retainer); err != nil {
			return batch, err
		}
		batch.State = "retained"
	}
	if batch.State == "retained" {
		if _, err := r.anchorBatchWith(ctx, lockConn, batch, *signature, publisher, retainer); err != nil {
			return batch, err
		}
		batch.State = "anchored"
	}
	return batch, nil
}
