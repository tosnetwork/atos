package toprotocol

import (
	"strings"
	"testing"

	"github.com/tosnetwork/atos/internal/store/memory"
)

func TestNewRejectsImplicitPlaintextRPC(t *testing.T) {
	_, err := New(Config{
		BaseURL: "http://127.0.0.1:9000", BearerToken: "token", Store: memory.New(),
	})
	if err == nil || !strings.Contains(err.Error(), "requires explicit insecure mode") {
		t.Fatalf("New() error = %v, want explicit plaintext rejection", err)
	}
}

func TestNewRequiresBearerToken(t *testing.T) {
	_, err := New(Config{
		BaseURL: "https://tos-protocol.internal", Store: memory.New(),
	})
	if err == nil || !strings.Contains(err.Error(), "bearer token is required") {
		t.Fatalf("New() error = %v, want missing bearer token error", err)
	}
}

func TestNewAcceptsExplicitLocalPlaintext(t *testing.T) {
	client, err := New(Config{
		BaseURL: "http://127.0.0.1:9000", BearerToken: "token",
		Insecure: true, Store: memory.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
