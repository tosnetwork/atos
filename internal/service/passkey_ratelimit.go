package service

import (
	"sync"
	"time"
)

// passkeyRateLimiter is a minimal in-memory sliding-window limiter guarding
// the two genuinely anonymous entry points (BeginRegistration/BeginLogin) --
// each ceremony-begin call writes a durable row (domain.WebAuthnCeremony)
// with no authentication precondition at all, so without this an attacker
// can flood passkey_ceremonies and mint unlimited principal_ids (each
// carrying the full default scope bundle and eventually a lazily-created
// ManagedAccount initial balance -- see AccountService.Get) purely by
// scripting HTTP requests, never touching the WebAuthn ceremony itself.
//
// This is deliberately simple (per-IP sliding window, not a distributed
// limiter, no CAPTCHA/invite-gating) -- it bounds scripted-flood velocity
// from a small number of IPs, which is what a single self-contained
// process can reasonably enforce on its own. It does not defend against a
// large botnet rotating many IPs; that requires infrastructure-level
// mitigation (WAF/CDN rate limiting) or a product decision to add
// CAPTCHA/invite-gating to signup, out of scope for this fix -- see
// docs/AUTH.md's own explicit scope boundary.
type passkeyRateLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
}

func newPasskeyRateLimiter() *passkeyRateLimiter {
	return &passkeyRateLimiter{attempts: make(map[string][]time.Time)}
}

// allow reports whether key has made fewer than max attempts within the
// trailing window ending at now, recording this attempt if so. Expired
// entries for key are pruned on every call, so the map only grows with
// distinct keys actually seen, never with stale timestamps.
func (l *passkeyRateLimiter) allow(key string, max int, window time.Duration, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := now.Add(-window)
	var kept []time.Time
	for _, t := range l.attempts[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= max {
		l.attempts[key] = kept
		return false
	}
	l.attempts[key] = append(kept, now)
	return true
}
