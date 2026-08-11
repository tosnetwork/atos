package service

import (
	"testing"
	"time"
)

func TestPasskeyRateLimiter_SlidingWindow(t *testing.T) {
	l := newPasskeyRateLimiter()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		if !l.allow("k", 3, time.Minute, base.Add(time.Duration(i)*time.Second)) {
			t.Fatalf("attempt %d should be allowed", i)
		}
	}
	if l.allow("k", 3, time.Minute, base.Add(3*time.Second)) {
		t.Fatal("4th attempt within the window should be rejected")
	}
	// Past the window, the earlier attempts have aged out.
	if !l.allow("k", 3, time.Minute, base.Add(90*time.Second)) {
		t.Fatal("an attempt after the window has fully elapsed should be allowed")
	}
}

func TestPasskeyRateLimiter_KeysAreIndependent(t *testing.T) {
	l := newPasskeyRateLimiter()
	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		if !l.allow("a", 5, time.Minute, now) {
			t.Fatalf("key a attempt %d should be allowed", i)
		}
	}
	if l.allow("a", 5, time.Minute, now) {
		t.Fatal("key a should now be exhausted")
	}
	if !l.allow("b", 5, time.Minute, now) {
		t.Fatal("a different key must not be affected by key a's exhausted quota")
	}
}

// TestPasskeyRateLimiter_PurgeStale is a regression test for a real P1: the
// limiter's map never removed keys that were only ever seen once (e.g. an
// attacker rotating through many distinct source values), growing without
// bound between individual keys' own opportunistic cleanup in allow.
func TestPasskeyRateLimiter_PurgeStale(t *testing.T) {
	l := newPasskeyRateLimiter()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// "stale" is seen once, long enough ago that it's outside the window
	// by the time purgeStale runs.
	l.allow("stale", 100, time.Minute, base)
	// "fresh" is seen recently.
	l.allow("fresh", 100, time.Minute, base.Add(90*time.Second))

	removed := l.purgeStale(time.Minute, base.Add(90*time.Second))
	if removed != 1 {
		t.Fatalf("purgeStale removed = %d, want 1 (only the stale key)", removed)
	}

	// The fresh key's quota must be untouched by the purge.
	if !l.allow("fresh", 2, time.Minute, base.Add(91*time.Second)) {
		t.Fatal("fresh key's second attempt should still be allowed (1 prior + this one = 2, at the limit)")
	}
	if l.allow("fresh", 2, time.Minute, base.Add(92*time.Second)) {
		t.Fatal("fresh key should now be at its limit")
	}

	// The purged stale key starts fresh again, as if never seen.
	if !l.allow("stale", 1, time.Minute, base.Add(200*time.Second)) {
		t.Fatal("a purged key should behave as if it had no prior attempts")
	}
}
