package financial

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
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
	client    *http.Client
}

func NewHTTPSigner(endpoint, keyID, algorithm string, timeout time.Duration) (*HTTPSigner, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || keyID == "" || (algorithm != "ed25519" && algorithm != "ecdsa_p256_sha256") || timeout <= 0 {
		return nil, errors.New("financial: invalid external signer configuration")
	}
	return &HTTPSigner{endpoint: endpoint, keyID: keyID, algorithm: algorithm, client: &http.Client{Timeout: timeout}}, nil
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

func (r *Repository) SignBatch(ctx context.Context, batch Batch, signer ExternalSigner, keyID, algorithm, trustedPublicKey string) (SignatureEnvelope, error) {
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
		return SignatureEnvelope{}, errors.New("financial: signer substituted key or algorithm")
	}
	envelope := SignatureEnvelope{"atos_financial_batch_signature_v1", batch.Manifest.BatchID, batch.ManifestDigest, digest,
		batch.Manifest.GatewayID, batch.Manifest.NetworkID, response.KeyID, response.Algorithm,
		response.Signature, response.PublicKey, response.SignedUnixMillis}
	if err := VerifySignature(envelope); err != nil {
		return SignatureEnvelope{}, err
	}
	raw, _ := json.Marshal(envelope)
	result, err := r.pool.Exec(ctx, `UPDATE financial_batches SET signing_key_id=$2,signature_envelope=$3,state='signed',updated_at=now()
 WHERE batch_id=$1 AND state IN ('created','signed') AND (signature_envelope IS NULL OR signature_envelope=$3)`, batch.Manifest.BatchID, keyID, raw)
	if err != nil {
		return SignatureEnvelope{}, err
	}
	if result.RowsAffected() != 1 {
		return SignatureEnvelope{}, ErrIdempotencyConflict
	}
	return envelope, nil
}

type EvidenceBundle struct {
	Version      string            `json:"version"`
	Manifest     BatchManifest     `json:"manifest"`
	ManifestCBOR string            `json:"manifest_cbor"`
	Commitments  []Commitment      `json:"commitments"`
	Signature    SignatureEnvelope `json:"signature"`
}

type Retainer interface {
	PutIfAbsent(context.Context, string, []byte, string) (string, error)
}

type HTTPRetainer struct {
	endpoint *url.URL
	client   *http.Client
}

func NewHTTPRetainer(endpoint string, timeout time.Duration) (*HTTPRetainer, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || timeout <= 0 {
		return nil, errors.New("financial: invalid WORM endpoint")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return &HTTPRetainer{parsed, &http.Client{Timeout: timeout}}, nil
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
	req.Header.Set("X-Content-SHA256", digest)
	response, err := r.client.Do(req)
	if err == nil {
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return response.Header.Get("X-Object-Version-ID"), nil
		}
		if response.StatusCode != http.StatusConflict && response.StatusCode != http.StatusPreconditionFailed {
			return "", fmt.Errorf("financial: WORM store returned HTTP %d", response.StatusCode)
		}
	}
	// Conflict and lost-response recovery both resolve the immutable object.
	head, headErr := http.NewRequestWithContext(ctx, http.MethodHead, endpoint.String(), nil)
	if headErr != nil {
		return "", errors.Join(err, headErr)
	}
	resolved, headErr := r.client.Do(head)
	if headErr != nil {
		return "", errors.Join(err, headErr)
	}
	defer resolved.Body.Close()
	if resolved.StatusCode != http.StatusOK || resolved.Header.Get("X-Content-SHA256") != digest {
		return "", errors.Join(err, ErrIdempotencyConflict)
	}
	return resolved.Header.Get("X-Object-Version-ID"), nil
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

func (r *Repository) RetainBatch(ctx context.Context, batch Batch, signature SignatureEnvelope, retainer Retainer) (string, string, error) {
	if retainer == nil {
		return "", "", errors.New("financial: retainer required")
	}
	bundle := EvidenceBundle{"atos_financial_evidence_bundle_v1", batch.Manifest,
		base64.StdEncoding.EncodeToString(batch.ManifestCBOR), batch.Commitments, signature}
	body, err := json.Marshal(bundle)
	if err != nil {
		return "", "", err
	}
	hash := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	key := fmt.Sprintf("atos-financial/v1/%s/%s/%d-%s.json", batch.Manifest.GatewayID, batch.Manifest.NetworkID, batch.Manifest.BatchSequence, batch.Manifest.BatchID)
	version, err := retainer.PutIfAbsent(ctx, key, body, digest)
	if err != nil {
		return "", "", err
	}
	result, err := r.pool.Exec(ctx, `UPDATE financial_batches SET retained_object_key=$2,retained_version_id=$3,state='retained',updated_at=now()
 WHERE batch_id=$1 AND state IN ('signed','retained') AND (retained_object_key='' OR retained_object_key=$2)`, batch.Manifest.BatchID, key, version)
	if err != nil {
		return "", "", err
	}
	if result.RowsAffected() != 1 {
		return "", "", ErrIdempotencyConflict
	}
	return key, version, nil
}

func (r *Repository) SealNext(ctx context.Context, signer ExternalSigner, keyID, algorithm, trustedPublicKey string, retainer Retainer, publisher AnchorPublisher, limit int) (Batch, error) {
	batch, signature, err := r.PendingBatch(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		batch, err = r.CreateBatch(ctx, limit)
	}
	if err != nil {
		return Batch{}, err
	}
	if batch.State == "created" {
		envelope, err := r.SignBatch(ctx, batch, signer, keyID, algorithm, trustedPublicKey)
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
		if _, _, err := r.RetainBatch(ctx, batch, *signature, retainer); err != nil {
			return batch, err
		}
		batch.State = "retained"
	}
	if batch.State == "retained" {
		if _, err := r.AnchorBatch(ctx, batch, *signature, publisher, retainer); err != nil {
			return batch, err
		}
		batch.State = "anchored"
	}
	return batch, nil
}
