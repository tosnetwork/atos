package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/tosnetwork/atos/internal/domain"
	"github.com/tosnetwork/atos/internal/service"
)

// TestOpenTaskProposedPriceNeverAuthoritative proves a provider's
// proposed_price is a non-authoritative hint only -- the winning Quote's
// price is always computed by QuoteService's own pricing rules from the
// Capability's committed price_hint, never taken from what the provider
// proposed.
func TestOpenTaskProposedPriceNeverAuthoritative(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_price_bypass", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_price", Title: "task", Input: map[string]any{},
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-price",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// The provider names an absurdly low price -- 0.01, versus the
	// Capability's real price_hint of 1.00.
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_price_bypass", TaskID: task.ID, CapabilityID: cap.ID,
		ProposedPrice: &domain.Money{Amount: "0.01", Currency: "USD"}, IdempotencyKey: "propose-price",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if p.ProposedPrice == nil || p.ProposedPrice.Amount != "0.01" {
		t.Fatalf("expected the proposed_price hint to be recorded as given, got %+v", p.ProposedPrice)
	}

	finalTask, _, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_price", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-price",
	})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	quote, err := h.quotes.Get(ctx, finalTask.BoundQuoteID)
	if err != nil {
		t.Fatalf("Get bound quote: %v", err)
	}
	// The real price must reflect the Capability's own price_hint (1.00
	// plus the standard fee), never the provider-proposed 0.01.
	if quote.Price.Subtotal != "1.00" {
		t.Fatalf("bound quote subtotal = %q, want %q (the capability's real price, not the provider's proposed_price)", quote.Price.Subtotal, "1.00")
	}
}

// TestOpenTaskAcceptUsesTaskRequestedTrustModeNotProviderChoice proves the
// winning Quote's trust mode is resolved from the TASK's own
// RequestedTrustMode (the owner's constraint) -- a proposal has no trust
// mode field at all, so a provider cannot express or bypass this by
// construction, but this test proves the wiring actually uses the task's
// value end to end.
func TestOpenTaskAcceptUsesTaskRequestedTrustModeNotProviderChoice(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_trust_mode", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_trust", Title: "task", Input: map[string]any{},
		RequestedTrustMode: domain.RequestedTrustManaged,
		ExpiresAt:          time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-trust",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_trust_mode", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-trust",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	finalTask, _, err := openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_trust", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-trust",
	})
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	quote, err := h.quotes.Get(ctx, finalTask.BoundQuoteID)
	if err != nil {
		t.Fatalf("Get bound quote: %v", err)
	}
	if quote.TrustMode != domain.TrustModeManaged {
		t.Fatalf("bound quote trust_mode = %s, want managed (the task owner's own requested_trust_mode)", quote.TrustMode)
	}
}

// TestOpenTaskAcceptRejectsPausedCapabilityProposal proves a proposal
// bound to a capability that has since been paused (disabled) can never
// win, even if the task owner tries to accept it.
func TestOpenTaskAcceptRejectsPausedCapabilityProposal(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_paused", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_paused", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-paused",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_paused", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-paused",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := h.capabilities.Update(ctx, cap.ID, "agt_paused", map[string]any{"status": "paused"}, "pause-paused-test"); err != nil {
		t.Fatalf("pause capability: %v", err)
	}
	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_paused", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-paused",
	})
	if err == nil {
		t.Fatal("expected accept against a paused capability to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskProposalStale {
		t.Fatalf("expected ErrOpenTaskProposalStale, got %v", err)
	}
}

// TestOpenTaskAcceptRejectsWithdrawnProposal proves a withdrawn proposal
// can never be accepted.
func TestOpenTaskAcceptRejectsWithdrawnProposal(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_withdrawn", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_withdrawn", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-withdrawn",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_withdrawn", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-withdrawn",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if _, err := openTasks.Withdraw(ctx, service.WithdrawProposalInput{
		ProviderID: "agt_withdrawn", ProposalID: p.ID,
	}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	_, _, err = openTasks.Accept(ctx, service.AcceptProposalInput{
		PrincipalID: "prn_withdrawn", TaskID: task.ID, ProposalID: p.ID, IdempotencyKey: "accept-withdrawn",
	})
	if err == nil {
		t.Fatal("expected accept against a withdrawn proposal to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrOpenTaskProposalWithdrawn {
		t.Fatalf("expected ErrOpenTaskProposalWithdrawn, got %v", err)
	}
}

// TestOpenTaskWithdrawRejectsNonOwningProvider proves a provider cannot
// withdraw another provider's proposal.
func TestOpenTaskWithdrawRejectsNonOwningProvider(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_withdraw_owner", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_wd", Title: "task", ExpiresAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "publish-wd",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_withdraw_owner", TaskID: task.ID, CapabilityID: cap.ID, IdempotencyKey: "propose-wd",
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	_, err = openTasks.Withdraw(ctx, service.WithdrawProposalInput{ProviderID: "agt_impostor", ProposalID: p.ID})
	if err == nil {
		t.Fatal("expected withdraw from a non-owning provider to fail")
	}
	derr, ok := err.(*domain.Error)
	if !ok || derr.Code != domain.ErrPermissionDenied {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

// TestOpenTaskPublicListingRedactsInputAndProposalMessage proves the
// public marketplace view never leaks owner-only OpenTask.Input or a
// proposal's private Message/ProposedPrice to a caller who is neither the
// task owner nor (for a proposal) its own submitting provider.
func TestOpenTaskPublicListingRedactsInputAndProposalMessage(t *testing.T) {
	ctx := context.Background()
	h := newHarness()
	openTasks := service.NewOpenTaskService(h.store(), h.quotes, h.jobs)

	cap := registerCapability(t, h, "agt_redact", "1.00")
	task, err := openTasks.Publish(ctx, service.PublishOpenTaskInput{
		PrincipalID: "prn_redact", Title: "task", Input: map[string]any{"secret": "do-not-leak"},
		ExpiresAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "publish-redact",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := openTasks.Propose(ctx, service.ProposeInput{
		ProviderID: "agt_redact", TaskID: task.ID, CapabilityID: cap.ID,
		Message: "private negotiation note", ProposedPrice: &domain.Money{Amount: "0.50", Currency: "USD"},
		IdempotencyKey: "propose-redact",
	}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// A stranger (not the owner, not the provider) reading the task.
	strangerView, err := openTasks.Get(ctx, "prn_someone_else", task.ID)
	if err != nil {
		t.Fatalf("Get as stranger: %v", err)
	}
	if strangerView.Input != nil {
		t.Fatalf("stranger view leaked Input: %+v", strangerView.Input)
	}

	// Public listing must also never include Input.
	listed, err := openTasks.ListPublic(ctx, 10)
	if err != nil {
		t.Fatalf("ListPublic: %v", err)
	}
	found := false
	for _, lt := range listed {
		if lt.ID == task.ID {
			found = true
			if lt.Input != nil {
				t.Fatalf("public listing leaked Input: %+v", lt.Input)
			}
		}
	}
	if !found {
		t.Fatal("expected the open task to appear in the public listing")
	}

	strangerProposals, err := openTasks.ListProposals(ctx, "prn_someone_else", task.ID)
	if err != nil {
		t.Fatalf("ListProposals as stranger: %v", err)
	}
	if len(strangerProposals) != 1 || strangerProposals[0].Message != "" {
		t.Fatalf("stranger view leaked proposal message: %+v", strangerProposals)
	}
	if strangerProposals[0].ProposedPrice != nil {
		t.Fatalf("stranger view leaked proposed_price: %+v", strangerProposals)
	}

	ownerProposals, err := openTasks.ListProposals(ctx, "prn_redact", task.ID)
	if err != nil {
		t.Fatalf("ListProposals as owner: %v", err)
	}
	if len(ownerProposals) != 1 || ownerProposals[0].Message != "private negotiation note" {
		t.Fatalf("owner should see the full proposal message: %+v", ownerProposals)
	}
}
