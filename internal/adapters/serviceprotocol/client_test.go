package toprotocol

import (
	"strings"
	"testing"
)

func TestNewRejectsImplicitPlaintextRPC(t *testing.T) {
	_, err := New(Config{BaseURL: "http://127.0.0.1:9000", BearerToken: "token"})
	if err == nil || !strings.Contains(err.Error(), "requires explicit insecure mode") {
		t.Fatalf("New() error = %v", err)
	}
}

func TestNewRequiresBearerToken(t *testing.T) {
	_, err := New(Config{BaseURL: "https://tos-service-protocol.internal"})
	if err == nil || !strings.Contains(err.Error(), "bearer token") {
		t.Fatalf("New() error = %v", err)
	}
}

func TestNewAcceptsExplicitLocalPlaintext(t *testing.T) {
	client, err := New(Config{BaseURL: "http://127.0.0.1:9000", BearerToken: "token", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}
