package service

import (
	"context"
	toscoremock "github.com/tosnetwork/atos/internal/adapters/toscore/mock"
	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/store/memory"
	"testing"
	"time"
)

func TestPortableProofRejectsManagedAndCrossPrincipal(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	core := toscoremock.New(s)
	svc := NewPortableProofService(s, core)
	r := domain.Receipt{ID: "r1", QuoteID: "q1", EscrowID: "e1", JobID: "j1", PrincipalID: "p1", TrustMode: domain.TrustModeManaged, CreatedAt: time.Now()}
	if e := s.PutReceipt(ctx, r); e != nil {
		t.Fatal(e)
	}
	if _, e := svc.Create(ctx, r.ID, "other"); !domainErrorIs(e, domain.ErrPermissionDenied) {
		t.Fatalf("cross-principal err=%v", e)
	}
	if _, e := svc.Create(ctx, r.ID, r.PrincipalID); !domainErrorIs(e, domain.ErrProofProfileUnavailable) {
		t.Fatalf("managed err=%v", e)
	}
}
