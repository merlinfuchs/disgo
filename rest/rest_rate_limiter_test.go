package rest

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

func newTestRateLimiter() (*rateLimiterImpl, *CompiledEndpoint) {
	limiter := &rateLimiterImpl{
		config:  defaultRateLimiterConfig(),
		buckets: map[string]*bucket{},
	}
	return limiter, GetMember.Compile(nil, 1234, 5678)
}

// testResponse carries a via header by default, the way anything that actually reached discord
// does. Drop it to pose as a cloudflare ban.
func testResponse(status int, headers map[string]string) *http.Response {
	header := http.Header{"Via": []string{"1.1 google"}}
	for k, v := range headers {
		if v == "" {
			header.Del(k)
			continue
		}
		header.Set(k, v)
	}
	return &http.Response{StatusCode: status, Header: header}
}

func TestRateLimiterUnlockRateLimited(t *testing.T) {
	t.Parallel()

	data := []struct {
		Name    string
		Headers map[string]string
		// a global limit stalls every bucket, a route limit only stalls its own
		ExpectGlobal  bool
		ExpectBackoff time.Duration
	}{
		{
			Name:          "global limit carries no bucket header",
			Headers:       map[string]string{"X-RateLimit-Global": "true", "Retry-After": "2"},
			ExpectGlobal:  true,
			ExpectBackoff: 2 * time.Second,
		},
		{
			Name:          "cloudflare ban carries neither bucket nor via header",
			Headers:       map[string]string{"Retry-After": "3", "via": ""},
			ExpectGlobal:  true,
			ExpectBackoff: 3 * time.Second,
		},
		{
			Name:          "route limit carries a bucket header",
			Headers:       map[string]string{"X-RateLimit-Bucket": "abc", "Retry-After": "0.75"},
			ExpectBackoff: 750 * time.Millisecond,
		},
		{
			// a proxy in front of the api can drop via, and that must not promote a route limit
			// into a process wide stall
			Name:          "route limit without a via header stays route scoped",
			Headers:       map[string]string{"X-RateLimit-Bucket": "abc", "Retry-After": "1", "via": ""},
			ExpectBackoff: time.Second,
		},
		{
			// better to wait slightly too long than to turn a throttle into a hard failure
			Name:          "missing retry after falls back to reset after",
			Headers:       map[string]string{"X-RateLimit-Bucket": "abc", "X-RateLimit-Reset-After": "0.5"},
			ExpectBackoff: 500 * time.Millisecond,
		},
		{
			Name:          "missing retry after and reset after falls back to a default",
			Headers:       map[string]string{"X-RateLimit-Bucket": "abc"},
			ExpectBackoff: defaultRetryAfter,
		},
	}

	for _, d := range data {
		t.Run(d.Name, func(t *testing.T) {
			t.Parallel()

			limiter, endpoint := newTestRateLimiter()
			if err := limiter.Wait(context.Background(), endpoint); err != nil {
				t.Fatalf("Wait: %v", err)
			}

			before := time.Now()
			if err := limiter.Unlock(endpoint, testResponse(http.StatusTooManyRequests, d.Headers)); err != nil {
				t.Fatalf("Unlock: %v", err)
			}

			b := limiter.getBucket(endpoint, false)
			got := limiter.globalReset()
			if !d.ExpectGlobal {
				if !got.IsZero() {
					t.Fatalf("global reset = %s, want none for a route limit", got)
				}
				if b.Remaining != 0 {
					t.Fatalf("bucket remaining = %d, want 0", b.Remaining)
				}
				got = b.Reset
			} else if b.Remaining == 0 {
				t.Fatal("a global limit must not exhaust the route bucket")
			}

			if backoff := got.Sub(before); backoff < d.ExpectBackoff || backoff > d.ExpectBackoff+time.Second {
				t.Fatalf("backoff = %s, want about %s", backoff, d.ExpectBackoff)
			}
		})
	}
}

// a successful response still has to update the bucket from its headers, and X-RateLimit-Reset-After
// is routinely fractional as a bucket drains
func TestRateLimiterUnlockSuccess(t *testing.T) {
	t.Parallel()

	limiter, endpoint := newTestRateLimiter()
	if err := limiter.Wait(context.Background(), endpoint); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	before := time.Now()
	err := limiter.Unlock(endpoint, testResponse(http.StatusOK, map[string]string{
		"X-RateLimit-Bucket":      "abc",
		"X-RateLimit-Limit":       "10",
		"X-RateLimit-Remaining":   "0",
		"X-RateLimit-Reset-After": "0.5",
	}))
	if err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	b := limiter.getBucket(endpoint, false)
	if b.ID != "abc" || b.Limit != 10 || b.Remaining != 0 {
		t.Fatalf("bucket = %+v, want id abc, limit 10, remaining 0", b)
	}
	if reset := b.Reset.Sub(before); reset < 500*time.Millisecond {
		t.Fatalf("bucket reset in %s, want at least 500ms", reset)
	}
	if got := limiter.globalReset(); !got.IsZero() {
		t.Fatalf("global reset = %s, want none for a successful response", got)
	}
}

// the global rate limit and the bucket rate limit both apply, so a request waits for whichever
// resets later
func TestWaitUntil(t *testing.T) {
	t.Parallel()

	now := time.Now()
	soon := now.Add(time.Second)
	later := now.Add(5 * time.Second)

	data := []struct {
		Name      string
		Remaining int
		Reset     time.Time
		Global    time.Time
		Expected  time.Time
	}{
		{Name: "exhausted bucket inside a longer global limit", Reset: soon, Global: later, Expected: later},
		{Name: "exhausted bucket outside the global limit", Reset: later, Global: soon, Expected: later},
		{Name: "bucket with requests left still waits for the global limit", Remaining: 5, Reset: soon, Global: later, Expected: later},
		{Name: "no limits at all", Remaining: 5, Reset: soon, Expected: time.Time{}},
		{Name: "expired bucket reset is ignored", Reset: now.Add(-time.Second), Expected: time.Time{}},
	}

	for _, d := range data {
		t.Run(d.Name, func(t *testing.T) {
			t.Parallel()

			got := waitUntil(&bucket{Remaining: d.Remaining, Reset: d.Reset}, d.Global, now)
			if !got.Equal(d.Expected) {
				t.Fatalf("waitUntil = %s, want %s", got, d.Expected)
			}
		})
	}
}

// requests in flight when a global limit trips come back with a counting down Retry-After, and the
// shortest of them must not cut the ban short
func TestSetGlobalResetOnlyExtends(t *testing.T) {
	t.Parallel()

	limiter, _ := newTestRateLimiter()
	longest := time.Now().Add(5 * time.Second)

	limiter.setGlobalReset(longest)
	limiter.setGlobalReset(time.Now().Add(time.Second))

	if got := limiter.globalReset(); !got.Equal(longest) {
		t.Fatalf("global reset = %s, want %s", got, longest)
	}

	limiter.clearGlobalReset()
	if got := limiter.globalReset(); !got.IsZero() {
		t.Fatalf("global reset = %s, want zero after a clear", got)
	}
}

// only meaningful under -race, where it catches the global reset being read in Wait and written in
// Unlock under two different bucket mutexes
func TestRateLimiterConcurrentGlobal(t *testing.T) {
	t.Parallel()

	limiter, _ := newTestRateLimiter()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			endpoint := GetMember.Compile(nil, i, 1)
			if err := limiter.Wait(context.Background(), endpoint); err != nil {
				t.Errorf("Wait: %v", err)
				return
			}
			if err := limiter.Unlock(endpoint, testResponse(http.StatusTooManyRequests, map[string]string{
				"X-RateLimit-Global": "true",
				"Retry-After":        "0",
			})); err != nil {
				t.Errorf("Unlock: %v", err)
			}
		}()
	}
	wg.Wait()

	if limiter.globalReset().IsZero() {
		t.Fatal("global rate limit was not recorded")
	}
}
