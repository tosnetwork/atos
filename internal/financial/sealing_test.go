package financial

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		message := request.Header.Get("X-ATOS-Retention-Timestamp") + "\n" + request.Method + "\n" + request.URL.EscapedPath() + "\n" + request.Header.Get("X-Content-SHA256")
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
			writer.Header().Set("X-Content-SHA256", digest)
			if !omitVersion.Load() {
				writer.Header().Set("X-Object-Version-ID", "locked-version-1")
			}
			writer.WriteHeader(http.StatusOK)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	if _, err := NewHTTPRetainer(server.URL, "short", time.Second); err == nil {
		t.Fatal("short unauthenticated retention credential was accepted")
	}
	retainer, err := NewHTTPRetainer(server.URL, secret, time.Second)
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
}
