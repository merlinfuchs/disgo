package rest

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

func newTestRateLimiter(t *testing.T) (*rateLimiterImpl, *CompiledEndpoint) {
	t.Helper()

	limiter, ok := NewRateLimiter().(*rateLimiterImpl)
	if !ok {
		t.Fatal("NewRateLimiter did not return a *rateLimiterImpl")
	}

	return limiter, GetMember.Compile(nil, 1234, 5678)
}

func response(status int, headers map[string]string) *http.Response {
	header := http.Header{}
	for k, v := range headers {
		header.Set(k, v)
	}
	return &http.Response{StatusCode: status, Header: header}
}

func TestRateLimiterUnlock(t *testing.T) {
	data := []struct {
		Name string
		// a cloudflare ban and a global rate limit both come back without a bucket header
		Response      *http.Response
		ExpectGlobal  bool
		ExpectRetryIn time.Duration
		// bucket state is only tracked for route specific limits
		ExpectBucketRemaining int
	}{
		{
			Name: "global rate limit without bucket header",
			Response: response(http.StatusTooManyRequests, map[string]string{
				"X-RateLimit-Global": "true",
				"Retry-After":        "2",
				"via":                "1.1 google",
			}),
			ExpectGlobal:          true,
			ExpectRetryIn:         2 * time.Second,
			ExpectBucketRemaining: 1,
		},
		{
			Name: "cloudflare rate limit without bucket header",
			Response: response(http.StatusTooManyRequests, map[string]string{
				"Retry-After": "3",
			}),
			ExpectGlobal:          true,
			ExpectRetryIn:         3 * time.Second,
			ExpectBucketRemaining: 1,
		},
		{
			Name: "route rate limit with bucket header",
			Response: response(http.StatusTooManyRequests, map[string]string{
				"X-RateLimit-Bucket": "abc",
				"Retry-After":        "1",
				"via":                "1.1 google",
			}),
			ExpectGlobal:          false,
			ExpectBucketRemaining: 0,
		},
	}

	for _, d := range data {
		t.Run(d.Name, func(t *testing.T) {
			limiter, endpoint := newTestRateLimiter(t)

			if err := limiter.Wait(context.Background(), endpoint); err != nil {
				t.Fatalf("Wait: %v", err)
			}

			before := time.Now()
			if err := limiter.Unlock(endpoint, d.Response); err != nil {
				t.Fatalf("Unlock: %v", err)
			}

			if d.ExpectGlobal {
				if limiter.global.IsZero() {
					t.Fatal("global rate limit was not recorded")
				}
				if got := limiter.global.Sub(before); got < d.ExpectRetryIn {
					t.Fatalf("global reset in %s, want at least %s", got, d.ExpectRetryIn)
				}
			} else if !limiter.global.IsZero() {
				t.Fatalf("global rate limit recorded for a route specific limit: %s", limiter.global)
			}

			if got := limiter.getBucket(endpoint, false).Remaining; got != d.ExpectBucketRemaining {
				t.Fatalf("bucket remaining = %d, want %d", got, d.ExpectBucketRemaining)
			}
		})
	}
}

// a successful response still has to update the bucket from its headers
func TestRateLimiterUnlockSuccess(t *testing.T) {
	limiter, endpoint := newTestRateLimiter(t)

	if err := limiter.Wait(context.Background(), endpoint); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	err := limiter.Unlock(endpoint, response(http.StatusOK, map[string]string{
		"X-RateLimit-Bucket":      "abc",
		"X-RateLimit-Limit":       "10",
		"X-RateLimit-Remaining":   "9",
		"X-RateLimit-Reset-After": "5",
		"via":                     "1.1 google",
	}))
	if err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	b := limiter.getBucket(endpoint, false)
	if b.ID != "abc" {
		t.Fatalf("bucket id = %q, want %q", b.ID, "abc")
	}
	if b.Limit != 10 || b.Remaining != 9 {
		t.Fatalf("bucket limit/remaining = %d/%d, want 10/9", b.Limit, b.Remaining)
	}
	if b.Reset.IsZero() {
		t.Fatal("bucket reset was not set")
	}
	if !limiter.global.IsZero() {
		t.Fatalf("global rate limit recorded for a successful response: %s", limiter.global)
	}
}

// the global rate limit and the bucket rate limit both apply, so a request has to wait for
// whichever resets later
func TestRateLimiterWaitUntil(t *testing.T) {
	now := time.Now()
	bucketReset := now.Add(1 * time.Second)
	globalReset := now.Add(5 * time.Second)

	data := []struct {
		Name      string
		Remaining int
		Reset     time.Time
		Global    time.Time
		Expected  time.Time
	}{
		{
			Name:      "exhausted bucket inside a longer global limit waits for the global one",
			Remaining: 0,
			Reset:     bucketReset,
			Global:    globalReset,
			Expected:  globalReset,
		},
		{
			Name:      "exhausted bucket outside the global limit waits for the bucket",
			Remaining: 0,
			Reset:     globalReset,
			Global:    bucketReset,
			Expected:  globalReset,
		},
		{
			Name:      "bucket with remaining requests still waits for the global limit",
			Remaining: 5,
			Reset:     bucketReset,
			Global:    globalReset,
			Expected:  globalReset,
		},
		{
			Name:      "no limits means no waiting",
			Remaining: 5,
			Reset:     bucketReset,
			Expected:  time.Time{},
		},
	}

	for _, d := range data {
		t.Run(d.Name, func(t *testing.T) {
			limiter, _ := newTestRateLimiter(t)
			limiter.setGlobalReset(d.Global)

			got := limiter.waitUntil(&bucket{Remaining: d.Remaining, Reset: d.Reset}, now)
			if !got.Equal(d.Expected) {
				t.Fatalf("waitUntil = %s, want %s", got, d.Expected)
			}
		})
	}
}

// Retry-After can carry decimals, the same as X-RateLimit-Reset-After
func TestRateLimiterFractionalRetryAfter(t *testing.T) {
	limiter, endpoint := newTestRateLimiter(t)

	if err := limiter.Wait(context.Background(), endpoint); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	before := time.Now()
	err := limiter.Unlock(endpoint, response(http.StatusTooManyRequests, map[string]string{
		"X-RateLimit-Bucket": "abc",
		"Retry-After":        "0.75",
		"via":                "1.1 google",
	}))
	if err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	b := limiter.getBucket(endpoint, false)
	if got := b.Reset.Sub(before); got < 700*time.Millisecond || got > time.Second {
		t.Fatalf("bucket reset in %s, want about 750ms", got)
	}
}

func TestRateLimiterConcurrentGlobal(t *testing.T) {
	limiter, _ := newTestRateLimiter(t)

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
			if err := limiter.Unlock(endpoint, response(http.StatusTooManyRequests, map[string]string{
				"X-RateLimit-Global": "true",
				"Retry-After":        "0",
				"via":                "1.1 google",
			})); err != nil {
				t.Errorf("Unlock: %v", err)
			}
		}()
	}
	wg.Wait()
}
