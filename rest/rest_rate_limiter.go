package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/sasha-s/go-csync"
)

const (
	// MaxRetries is the maximum number of retries the client should do
	MaxRetries = 10
	// CleanupInterval is the interval at which the rate limiter cleans up old buckets
	CleanupInterval = time.Second * 10
	// defaultRetryAfter is how long we back off for when a rate limited response doesn't tell us
	defaultRetryAfter = time.Second
)

// RateLimiter can be used to supply your own rate limit implementation
type RateLimiter interface {
	// MaxRetries returns the maximum number of retries the client should do
	MaxRetries() int

	// Close gracefully closes the RateLimiter.
	// If the context deadline is exceeded, the RateLimiter will be closed immediately.
	Close(ctx context.Context)

	// Reset resets the rate limiter to its initial state
	Reset()

	// Wait waits for the given bucket to be available for new requests & locks it
	Wait(ctx context.Context, endpoint *CompiledEndpoint) error

	// Unlock unlocks the given bucket and calculates the rate limit for the next request
	Unlock(endpoint *CompiledEndpoint, rs *http.Response) error
}

// NewRateLimiter return a new default RateLimiter with the given RateLimiterConfigOpt(s).
func NewRateLimiter(opts ...RateLimiterConfigOpt) RateLimiter {
	cfg := defaultRateLimiterConfig()
	cfg.apply(opts)

	rateLimiter := &rateLimiterImpl{
		config:  cfg,
		buckets: map[string]*bucket{},
	}

	go rateLimiter.cleanup()

	return rateLimiter
}

type rateLimiterImpl struct {
	config rateLimiterConfig

	// global Rate Limit
	globalMu sync.Mutex
	global   time.Time

	// Hash + Major Parameter -> bucket
	buckets   map[string]*bucket
	bucketsMu sync.Mutex
}

func (l *rateLimiterImpl) MaxRetries() int {
	return l.config.MaxRetries
}

func (l *rateLimiterImpl) globalReset() time.Time {
	l.globalMu.Lock()
	defer l.globalMu.Unlock()
	return l.global
}

// setGlobalReset extends the global rate limit, it never shortens it. Requests already in flight
// when the limit trips come back over the following seconds with a counting down Retry-After, and
// the shortest of those must not be allowed to end a ban an earlier one set.
func (l *rateLimiterImpl) setGlobalReset(reset time.Time) {
	l.globalMu.Lock()
	defer l.globalMu.Unlock()
	if reset.After(l.global) {
		l.global = reset
	}
}

func (l *rateLimiterImpl) clearGlobalReset() {
	l.globalMu.Lock()
	defer l.globalMu.Unlock()
	l.global = time.Time{}
}

// retryAfter is how long to back off for after a rate limited response. Retry-After should always
// be there, but an edge error page or a proxy can return a 429 without one, and turning a throttle
// into a permanent failure is worse than waiting a little too long.
func (l *rateLimiterImpl) retryAfter(headers ...string) time.Duration {
	for _, header := range headers {
		if header == "" {
			continue
		}
		seconds, err := strconv.ParseFloat(header, 64)
		if err != nil {
			l.config.Logger.Warn("invalid retry after header", slog.String("value", header))
			continue
		}
		return secondsToDuration(seconds)
	}
	return defaultRetryAfter
}

// secondsToDuration converts a header value in seconds to a Duration. Retry-After and
// X-RateLimit-Reset-After both carry decimals, so the multiply has to happen before the cast.
func secondsToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

// waitUntil returns the time at which the next request on this bucket may be sent. The global rate
// limit applies on top of the per bucket one, so whichever resets later wins.
func waitUntil(b *bucket, globalReset time.Time, now time.Time) time.Time {
	var until time.Time
	if b.Remaining == 0 && b.Reset.After(now) {
		until = b.Reset
	}
	if globalReset.After(until) {
		until = globalReset
	}
	return until
}

func (l *rateLimiterImpl) cleanup() {
	ticker := time.NewTicker(l.config.CleanupInterval)
	for range ticker.C {
		l.doCleanup()
	}
}

func (l *rateLimiterImpl) doCleanup() {
	l.bucketsMu.Lock()
	defer l.bucketsMu.Unlock()
	before := len(l.buckets)
	now := time.Now()
	for hash, b := range l.buckets {
		if !b.mu.TryLock() {
			continue
		}
		if b.Reset.Before(now) {
			l.config.Logger.Debug("cleaning up bucket", slog.String("hash", hash), slog.String("id", b.ID), slog.Time("reset", b.Reset))
			delete(l.buckets, hash)
		}
		b.mu.Unlock()
	}
	if before != len(l.buckets) {
		l.config.Logger.Debug("cleaned up rate limit buckets", slog.Int("before", before), slog.Int("after", len(l.buckets)), slog.Int("removed", before-len(l.buckets)))
	}
}

func (l *rateLimiterImpl) Close(ctx context.Context) {
	var wg sync.WaitGroup
	l.bucketsMu.Lock()
	for i := range l.buckets {
		wg.Add(1)
		b := l.buckets[i]
		go func() {
			_ = b.mu.CLock(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (l *rateLimiterImpl) Reset() {
	l.bucketsMu.Lock()
	defer l.bucketsMu.Unlock()

	l.clearGlobalReset()
	clear(l.buckets)
}

func (l *rateLimiterImpl) getRouteHash(endpoint *CompiledEndpoint) string {
	hash := endpoint.Endpoint.Method + "+" + endpoint.Endpoint.Route

	if endpoint.MajorParams != "" {
		hash += "+" + endpoint.MajorParams
	}
	return hash
}

func (l *rateLimiterImpl) getBucket(endpoint *CompiledEndpoint, create bool) *bucket {
	hash := l.getRouteHash(endpoint)

	l.config.Logger.Debug("locking buckets")
	l.bucketsMu.Lock()
	defer func() {
		l.config.Logger.Debug("unlocking buckets")
		l.bucketsMu.Unlock()
	}()
	b, ok := l.buckets[hash]
	if !ok {
		if !create {
			return nil
		}

		b = &bucket{
			Remaining: 1,
			// we don't know the limit yet
			Limit: -1,
		}
		l.buckets[hash] = b
	}
	return b
}

func (l *rateLimiterImpl) Wait(ctx context.Context, endpoint *CompiledEndpoint) error {
	b := l.getBucket(endpoint, true)
	l.config.Logger.Debug("locking rest bucket", slog.String("id", b.ID), slog.Int("limit", b.Limit), slog.Int("remaining", b.Remaining), slog.Time("reset", b.Reset))
	if err := b.mu.CLock(ctx); err != nil {
		return err
	}

	now := time.Now()
	until := waitUntil(b, l.globalReset(), now)

	if until.After(now) {
		// TODO: do we want to return early when we know the rate limit bigger than ctx deadline?
		if deadline, ok := ctx.Deadline(); ok && until.After(deadline) {
			b.mu.Unlock()
			return context.DeadlineExceeded
		}

		select {
		case <-ctx.Done():
			b.mu.Unlock()
			return ctx.Err()
		case <-time.After(until.Sub(now)):
		}
	}
	return nil
}

func (l *rateLimiterImpl) Unlock(endpoint *CompiledEndpoint, rs *http.Response) error {
	b := l.getBucket(endpoint, false)
	if b == nil {
		return nil
	}
	defer func() {
		l.config.Logger.Debug("unlocking rest bucket", slog.String("id", b.ID), slog.Int("limit", b.Limit), slog.Int("remaining", b.Remaining), slog.Time("reset", b.Reset))
		b.mu.Unlock()
	}()

	// no response provided means we can't update anything and just unlock it
	if rs == nil || rs.Header == nil {
		return nil
	}
	global := rs.Header.Get("X-RateLimit-Global") != ""
	bucketHeader := rs.Header.Get("X-RateLimit-Bucket")
	// a cloudflare ban never reaches discord, so it carries neither of these. the bucket header is
	// positive proof the response came from discord's own limiter, and it has to win over the
	// absence of via, which a proxy in front of the api drops for reasons of its own.
	cloudflare := bucketHeader == "" && rs.Header.Get("via") == ""
	remainingHeader := rs.Header.Get("X-RateLimit-Remaining")
	limitHeader := rs.Header.Get("X-RateLimit-Limit")
	resetHeader := rs.Header.Get("X-RateLimit-Reset")
	resetAfterHeader := rs.Header.Get("X-RateLimit-Reset-After")
	retryAfterHeader := rs.Header.Get("Retry-After")

	l.config.Logger.Debug("ratelimit response headers", slog.Int("code", rs.StatusCode), slog.Bool("global", global), slog.Bool("cloudflare", cloudflare), slog.String("remaining", remainingHeader), slog.String("limit", limitHeader), slog.String("reset", resetHeader), slog.String("reset_after", resetAfterHeader), slog.String("retry_after", retryAfterHeader))

	if bucketHeader != "" {
		b.ID = bucketHeader
	}

	// we hit a rate limit. let's see if it was a global/cloudflare one or a route specific one.
	// global and cloudflare responses carry no bucket header, so this runs before the bucket
	// header check below.
	if rs.StatusCode == http.StatusTooManyRequests {
		retryAfter := l.retryAfter(retryAfterHeader, resetAfterHeader)
		reset := time.Now().Add(retryAfter)

		if global || cloudflare {
			l.setGlobalReset(reset)
			l.config.Logger.Warn("global rate limit exceeded", slog.Bool("cloudflare", cloudflare), slog.Duration("retry_after", retryAfter))
		} else {
			b.Remaining = 0
			b.Reset = reset
			l.config.Logger.Warn("rate limit exceeded", slog.String("endpoint", endpoint.URL), slog.Duration("retry_after", retryAfter))
		}
		return nil
	}

	// if we don't have a bucket header, we can't update anything
	if bucketHeader == "" {
		return nil
	}

	if limitHeader != "" {
		limit, err := strconv.Atoi(limitHeader)
		if err != nil {
			return fmt.Errorf("invalid limit %s: %w", limitHeader, err)
		}
		b.Limit = limit
	}

	if remainingHeader != "" {
		remaining, err := strconv.Atoi(remainingHeader)
		if err != nil {
			return fmt.Errorf("invalid remaining %s: %w", remainingHeader, err)
		}
		b.Remaining = remaining
	}

	// we prioritize the reset after header over the reset header as it's more accurate due to clock differences
	if resetAfterHeader != "" {
		resetAfter, err := strconv.ParseFloat(resetAfterHeader, 64)
		if err != nil {
			return fmt.Errorf("invalid reset after %s: %w", resetAfterHeader, err)
		}

		b.Reset = time.Now().Add(secondsToDuration(resetAfter))
	} else if resetHeader != "" {
		reset, err := strconv.ParseFloat(resetHeader, 64)
		if err != nil {
			return fmt.Errorf("invalid reset %s: %w", resetHeader, err)
		}

		sec := int64(reset)
		b.Reset = time.Unix(sec, int64((reset-float64(sec))*float64(time.Second)))
	} else {
		return fmt.Errorf("no reset or reset after header found in response")
	}
	return nil
}

type bucket struct {
	mu        csync.Mutex
	ID        string
	Reset     time.Time
	Remaining int
	Limit     int
}
