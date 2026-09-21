package rest

import (
	"context"
	"net/http"
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
