package quotesource

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	nativev1 "github.com/tosnetwork/tos-protocol/gen/atos/native/v1"
)

type resolverStub struct{}

func (resolverStub) ResolveNativeState(context.Context, *nativev1.ResolveNativeStateRequest) (*nativev1.ResolveNativeStateResponse, error) {
	return nil, nil
}

func TestLoadRejectsNonPrivateOrAmbiguousProfile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "profile.json")
	if err := os.WriteFile(path, []byte(`{"schema":"atos.provider-quote-profile.v1","unknown":true}`), 0644); err != nil {
		t.Fatal(err)
	}
	network := &nativev1.NetworkDomain{NetworkId: "test", GenesisRootHash: "sha256:" + strings.Repeat("11", 32),
		GenesisFileHash: "sha256:" + strings.Repeat("22", 32)}
	if _, err := Load(path, resolverStub{}, network, "tvm-cell-sha256:"+strings.Repeat("33", 32), 0); err == nil {
		t.Fatal("group-readable provider profile accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, resolverStub{}, network, "tvm-cell-sha256:"+strings.Repeat("33", 32), 0); err == nil {
		t.Fatal("unknown provider profile field accepted")
	}
}
