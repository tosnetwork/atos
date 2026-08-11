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
// large botnet rotating many source IPs; that requires infrastructure-level
// mitigation (WAF/CDN rate limiting) or a product decision to add
// CAPTCHA/invite-gating to signup, out of scope for this fix -- see
// docs/AUTH.md's own explicit scope boundary. The rate-limit key is the
// caller's real TCP-connection source IP (internal/httpapi.clientIP
// deliberately does not trust any client-suppliable proxy header, for the
// same reason), so it cannot be trivially spoofed per-request the way a
// header value could.
type passkeyRateLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
}

func newPasskeyRateLimiter() *passkeyRateLimiter {
	return &passkeyRateLimiter{attempts: make(map[string][]time.Time)}
}

// allow reports whether key has made fewer than max attempts within the
// trailing window ending at now, recording this attempt if so. Expired
// entries for key are pruned on every call; a key left with zero entries is
// deleted outright rather than stored as an empty slice, so a key that is
// never seen again does not linger forever between purgeStale sweeps.
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
	kept = append(kept, now)
	l.attempts[key] = kept
	return true
}

// purgeStale removes every key whose most recent attempt is already
// outside window as of now -- called periodically (see
// PasskeyService.PurgeExpiredCeremonies's sibling wiring in
// cmd/api/main.go) so a flood of distinct keys that are each only ever
// seen once (e.g. spoofed source addresses, before clientIP's fix to key
// on the real TCP source instead) cannot grow this map without bound
// between individual keys' own opportunistic cleanup in allow.
func (l *passkeyRateLimiter) purgeStale(window time.Duration, now time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := now.Add(-window)
	removed := 0
	for key, attempts := range l.attempts {
		stale := true
		for _, t := range attempts {
			if t.After(cutoff) {
				stale = false
				break
			}
		}
		if stale {
			delete(l.attempts, key)
			removed++
		}
	}
	return removed
}
