package domain

import "testing"

func TestSelectBinding_PrefersEligibleTrustMode(t *testing.T) {
	bindings := []CapabilityBinding{
		{Transport: AdapterHTTP, EndpointRef: "http://a", EligibleTrustModes: []TrustMode{TrustModeVerified}},
		{Transport: AdapterMCP, EndpointRef: "http://b#tool", EligibleTrustModes: []TrustMode{TrustModeManaged}},
	}
	chosen, ok := SelectBinding(bindings, TrustModeManaged)
	if !ok || chosen.Transport != AdapterMCP {
		t.Fatalf("chosen = %+v, want the MCP binding (eligible for managed)", chosen)
	}
}

func TestSelectBinding_FallsBackToFirstWhenNoneEligible(t *testing.T) {
	bindings := []CapabilityBinding{{Transport: AdapterHTTP, EndpointRef: "http://a"}}
	chosen, ok := SelectBinding(bindings, TrustModeManaged)
	if !ok || chosen.Transport != AdapterHTTP {
		t.Fatalf("chosen = %+v", chosen)
	}
}

func TestSelectBinding_EmptyBindingsNotOK(t *testing.T) {
	_, ok := SelectBinding(nil, TrustModeManaged)
	if ok {
		t.Fatal("expected ok=false for no bindings at all")
	}
}
