package financial

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPRetainerAuthenticatesAndRequiresImmutableVersion(t *testing.T) {
	secret := strings.Repeat("retention-secret-", 2)
	body := []byte(`{"evidence":"sealed"}`)
	hash := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	var requests atomic.Int32
	var omitVersion atomic.Bool
	var lockMode atomic.Value
	var retentionSeconds atomic.Int64
	var headStatus atomic.Int64
	lockMode.Store("COMPLIANCE")
	retentionSeconds.Store(int64((2 * time.Hour).Seconds()))
	headStatus.Store(http.StatusOK)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		target := request.URL.EscapedPath()
		if request.URL.RawQuery != "" {
			target += "?" + request.URL.RawQuery
		}
		message := request.Header.Get("X-ATOS-Retention-Timestamp") + "\n" + request.Method + "\n" + target + "\n" + request.Header.Get("X-Content-SHA256")
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte(message))
		expected := "hmac-sha256=" + hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(request.Header.Get("X-ATOS-Retention-Signature")), []byte(expected)) {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		requests.Add(1)
		switch request.Method {
		case http.MethodPut:
			payload, _ := io.ReadAll(request.Body)
			payloadHash := sha256.Sum256(payload)
			if "sha256:"+hex.EncodeToString(payloadHash[:]) != digest {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			writer.WriteHeader(http.StatusCreated)
		case http.MethodHead:
			if status := int(headStatus.Load()); status != http.StatusOK {
				writer.WriteHeader(status)
				return
			}
			writer.Header().Set("X-Content-SHA256", digest)
			if !omitVersion.Load() {
				writer.Header().Set("X-Object-Version-ID", "locked-version-1")
			}
			writer.Header().Set("X-Object-Lock-Mode", lockMode.Load().(string))
			writer.Header().Set("X-Object-Retain-Until", time.Now().UTC().Add(time.Duration(retentionSeconds.Load())*time.Second).Format(time.RFC3339))
			writer.WriteHeader(http.StatusOK)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	if _, err := NewHTTPRetainer(server.URL, "short", time.Second, time.Hour); err == nil {
		t.Fatal("short unauthenticated retention credential was accepted")
	}
	retainer, err := NewHTTPRetainer(server.URL, secret, time.Second, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	version, err := retainer.PutIfAbsent(context.Background(), "evidence/1.json", body, digest)
	if err != nil || version != "locked-version-1" || requests.Load() != 2 {
		t.Fatalf("authenticated retention version=%q requests=%d error=%v", version, requests.Load(), err)
	}
	omitVersion.Store(true)
	if _, err := retainer.PutIfAbsent(context.Background(), "evidence/2.json", body, digest); err == nil {
		t.Fatal("retention succeeded without an immutable object version")
	}
	omitVersion.Store(false)
	deadline := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	if _, err := retainWithProof(context.Background(), retainer, "evidence/3.json", body, digest, deadline); err != nil {
		t.Fatalf("valid COMPLIANCE retention was rejected: %v", err)
	}
	// A retry uses the original durable deadline instead of extending it from
	// the retry time. This is the crash window after PUT but before retained.
	if _, err := retainWithProof(context.Background(), retainer, "evidence/3.json", body, digest, deadline); err != nil {
		t.Fatalf("stable retention deadline did not converge on retry: %v", err)
	}
	lockMode.Store("GOVERNANCE")
	if _, err := retainWithProof(context.Background(), retainer, "evidence/4.json", body, digest, deadline); err == nil {
		t.Fatal("production state transition accepted non-COMPLIANCE retention")
	}
	lockMode.Store("COMPLIANCE")
	retentionSeconds.Store(int64((30 * time.Minute).Seconds()))
	if _, err := retainWithProof(context.Background(), retainer, "evidence/5.json", body, digest, deadline); err == nil {
		t.Fatal("production state transition accepted retention below configured minimum")
	}
	retentionSeconds.Store(int64((2 * time.Hour).Seconds()))
	headStatus.Store(http.StatusServiceUnavailable)
	if _, err := retainer.ResolveRetention(context.Background(), "evidence/3.json", "locked-version-1", digest); err == nil || errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("temporary retention failure misclassified as integrity conflict: %v", err)
	}
	headStatus.Store(http.StatusNotFound)
	if _, err := retainer.ResolveRetention(context.Background(), "evidence/3.json", "locked-version-1", digest); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("missing exact version was not an integrity conflict: %v", err)
	}
}
