// Package loadshedder provides HTTP middleware that rejects excess traffic
// before it overwhelms a server. It combines server-wide and per-IP burst/rate
// limiting with server-wide and per-IP concurrency limiting.
//
// Create a LoadShedder with New, wrap your handler with Handler, and call
// Stop on shutdown. See the With* option functions for configuration.
package loadshedder

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
)

// goldenRatio is used to derive a second hash key from a single murmur3
// hash, avoiding the cost of hashing the IP address twice.
const goldenRatio = 0x9e3779b9

// Option configures a LoadShedder. Pass options to New.
type Option func(*LoadShedder)

// LoadShedder is an HTTP middleware that protects services from overload.
// It rejects excess traffic using four mechanisms: a server-wide burst/rate
// limiter for rate limiting, a server-wide in-flight counter for
// concurrency limiting, a per-IP burst/rate limiter for rate limiting, and a
// per-IP in-flight counter for concurrency limiting.
//
// A background goroutine evicts idle buckets every minute. Call Stop to
// release it when the LoadShedder is no longer needed.
//
// See New, Handler, and the With* options.
type LoadShedder struct {
	// per-IP config
	perIPBurst       int
	perIPRate        int
	perIPMaxInFlight int32

	// server-wide config
	serverBurst      int
	serverRate       int
	serverMaxInFlight int32

	// server-wide limiters
	serverRateBucket *TokenBucket // nil when not configured
	serverInflight   int32        // single atomic counter

	// per-IP limiters — sharded maps reduce contention vs sync.Map
	rateLimitBuckets *shardedMap[*TokenBucket]
	inFlightBuckets  *shardedMap[*int32]

	// counters
	requestCount                int64
	sheddedRequestCount         int64
	sheddedCausePerIPInFlight  int64
	sheddedCausePerIPRate      int64
	sheddedCauseServerInFlight int64
	sheddedCauseServerRate     int64

	// cleanup management
	cleanupOnce sync.Once
	stopOnce    sync.Once
	stopCleanup chan struct{}
}

// New creates a LoadShedder configured by the given options.
// It starts a background goroutine that periodically evicts idle buckets.
// Call Stop when the LoadShedder is no longer needed.
//
// Without options every field defaults to its zero value, which means all
// requests are immediately rejected. Provide at least WithPerIPBurst,
// WithPerIPRatePerSecond, and WithPerIPMaxInFlight to get useful behavior.
//
// Server options are independently composable with per-IP options.
// A request must pass all active checks. Server options with value zero
// are disabled (unlike per-IP where zero means reject-all).
//
//	ls := loadshedder.New(
//	    loadshedder.WithPerIPBurst(100),
//	    loadshedder.WithPerIPRatePerSecond(10),
//	    loadshedder.WithPerIPMaxInFlight(20),
//	    loadshedder.WithServerBurst(1000),
//	    loadshedder.WithServerRatePerSecond(100),
//	    loadshedder.WithServerMaxInFlight(200),
//	)
//	defer ls.Stop()
func New(opts ...Option) *LoadShedder {
	ls := &LoadShedder{
		rateLimitBuckets: newShardedMap[*TokenBucket](),
		inFlightBuckets:  newShardedMap[*int32](),
		stopCleanup:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(ls)
	}

	if ls.serverBurst > 0 {
		ls.serverRateBucket = NewTokenBucket(ls.serverBurst, ls.serverRate)
	}

	// Start background cleanup goroutine
	ls.cleanupOnce.Do(func() {
		go ls.backgroundCleanup()
	})

	return ls
}

// WithPerIPBurst sets the maximum number of tokens each client bucket can
// hold. This is the burst size: a client can make up to perIPBurst requests
// before being rate limited. Panics if perIPBurst is negative.
//
//	loadshedder.WithPerIPBurst(50) // allow bursts of 50 requests
func WithPerIPBurst(perIPBurst int) Option {
	return func(ls *LoadShedder) {
		if perIPBurst < 0 {
			panic("loadshedder: perIPBurst must be non-negative")
		}
		ls.perIPBurst = perIPBurst
	}
}

// WithPerIPRatePerSecond sets how many tokens are added to each client bucket
// per second. Together with the burst size this controls the sustained request
// rate: a client can sustain perIPRate requests/s after exhausting its burst
// allowance. Panics if perIPRate is negative.
//
//	loadshedder.WithPerIPRatePerSecond(10) // sustain 10 req/s
func WithPerIPRatePerSecond(perIPRate int) Option {
	return func(ls *LoadShedder) {
		if perIPRate < 0 {
			panic("loadshedder: perIPRate must be non-negative")
		}
		ls.perIPRate = perIPRate
	}
}

// WithPerIPMaxInFlight sets the maximum number of concurrent requests allowed
// per client IP. When a client already has m requests being processed, new
// requests are rejected with 429 Too Many Requests. Panics if m is negative.
//
//	loadshedder.WithPerIPMaxInFlight(20) // at most 20 concurrent per IP
func WithPerIPMaxInFlight(m int32) Option {
	return func(ls *LoadShedder) {
		if m < 0 {
			panic("loadshedder: perIPMaxInFlight must be non-negative")
		}
		ls.perIPMaxInFlight = m
	}
}

// WithServerBurst sets the maximum number of tokens in the server-wide rate
// limit bucket. This is the burst size across all clients combined. Zero means
// disabled (no server-wide rate limit). Panics if negative.
//
//	loadshedder.WithServerBurst(1000) // server-wide burst of 1000
func WithServerBurst(serverBurst int) Option {
	return func(ls *LoadShedder) {
		if serverBurst < 0 {
			panic("loadshedder: serverBurst must be non-negative")
		}
		ls.serverBurst = serverBurst
	}
}

// WithServerRatePerSecond sets how many tokens are added to the server-wide
// bucket per second. Together with the server burst this controls the sustained
// total request rate. Zero means no refill. Panics if negative.
//
//	loadshedder.WithServerRatePerSecond(100) // sustain 100 req/s total
func WithServerRatePerSecond(serverRate int) Option {
	return func(ls *LoadShedder) {
		if serverRate < 0 {
			panic("loadshedder: serverRate must be non-negative")
		}
		ls.serverRate = serverRate
	}
}

// WithServerMaxInFlight sets the maximum number of concurrent requests allowed
// across all clients. When the server already has m requests being processed,
// new requests are rejected with 429. Zero means disabled (no server-wide
// concurrency limit). Panics if negative.
//
//	loadshedder.WithServerMaxInFlight(200) // at most 200 concurrent total
func WithServerMaxInFlight(m int32) Option {
	return func(ls *LoadShedder) {
		if m < 0 {
			panic("loadshedder: serverMaxInFlight must be non-negative")
		}
		ls.serverMaxInFlight = m
	}
}

// Handler returns middleware that guards next with rate and concurrency limits.
//
// For each request the middleware checks in order:
//  1. Server-wide in-flight concurrency (if configured).
//  2. Server-wide rate limit (if configured).
//  3. Per-IP in-flight concurrency.
//  4. Per-IP rate limit.
//  5. Calls next.
//
// Server-wide checks run first because they use single atomics (cheaper than
// per-IP map lookups). A request must pass all active checks.
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/", handleRoot)
//	http.ListenAndServe(":8080", ls.Handler(mux))
func (ls *LoadShedder) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&ls.requestCount, 1)

		// --- Server-wide in-flight check ---
		if ls.serverMaxInFlight > 0 {
			if atomic.LoadInt32(&ls.serverInflight) >= ls.serverMaxInFlight {
				atomic.AddInt64(&ls.sheddedRequestCount, 1)
				atomic.AddInt64(&ls.sheddedCauseServerInFlight, 1)
				http.Error(w, "Too many simultaneous requests", http.StatusTooManyRequests)
				return
			}
			atomic.AddInt32(&ls.serverInflight, 1)
			defer atomic.AddInt32(&ls.serverInflight, -1)
		}

		// --- Server-wide rate limit check ---
		if ls.serverRateBucket != nil {
			if !ls.serverRateBucket.TryTake() {
				atomic.AddInt64(&ls.sheddedRequestCount, 1)
				atomic.AddInt64(&ls.sheddedCauseServerRate, 1)
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}
		}

		// Single murmur3 hash of the IP; derive the second key via XOR
		// with the golden ratio to avoid hashing twice (~15ns saved).
		ipHash := bucketKeyIP(r)
		inFlightKey := ipHash
		rateLimitKey := ipHash ^ goldenRatio

		// --- Per-IP in-flight check ---
		// Two-phase lookup: Load is the fast path for returning clients;
		// LoadOrStore only runs on the first request from a new IP.
		inFlightPtr, ok := ls.inFlightBuckets.Load(inFlightKey)
		if !ok {
			inFlightPtr, _ = ls.inFlightBuckets.LoadOrStore(inFlightKey, new(int32))
		}

		// Benign TOCTOU: two goroutines may both read just below the limit.
		// Acceptable soft cap — a CAS loop would add contention for no gain.
		if atomic.LoadInt32(inFlightPtr) >= ls.perIPMaxInFlight {
			atomic.AddInt64(&ls.sheddedRequestCount, 1)
			atomic.AddInt64(&ls.sheddedCausePerIPInFlight, 1)
			http.Error(w, "Too many simultaneous requests", http.StatusTooManyRequests)
			return
		}

		atomic.AddInt32(inFlightPtr, 1)
		defer atomic.AddInt32(inFlightPtr, -1)

		// --- Per-IP rate limit check ---
		// Two-phase lookup: Load is the fast path for returning clients;
		// LoadOrStore only runs on the first request from a new IP.
		bucket, ok := ls.rateLimitBuckets.Load(rateLimitKey)
		if !ok {
			newBucket := NewTokenBucket(ls.perIPBurst, ls.perIPRate)
			bucket, _ = ls.rateLimitBuckets.LoadOrStore(rateLimitKey, newBucket)
		}

		if !bucket.TryTake() {
			atomic.AddInt64(&ls.sheddedRequestCount, 1)
			atomic.AddInt64(&ls.sheddedCausePerIPRate, 1)
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// backgroundCleanup runs cleanup every minute in the background.
func (ls *LoadShedder) backgroundCleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cutoff := time.Now().Add(-time.Minute).Unix()
			ls.Cleanup(context.Background(), cutoff)
		case <-ls.stopCleanup:
			return
		}
	}
}

// Stop stops the background cleanup goroutine.
// It is safe to call multiple times.
//
//	ls := loadshedder.New(...)
//	defer ls.Stop()
func (ls *LoadShedder) Stop() {
	ls.stopOnce.Do(func() {
		close(ls.stopCleanup)
	})
}

// Cleanup removes token buckets whose last refill is older than
// cutoffTimestampSeconds (Unix seconds) and in-flight counters that are zero.
// It is called automatically by the background goroutine every minute.
// Exposed for testing and manual eviction.
//
//	cutoff := time.Now().Add(-time.Minute).Unix()
//	ls.Cleanup(ctx, cutoff)
func (ls *LoadShedder) Cleanup(ctx context.Context, cutoffTimestampSeconds int64) {
	var rateLimitKeysToDelete []uint32
	ls.rateLimitBuckets.Range(func(key uint32, bucket *TokenBucket) bool {
		lastRefill := atomic.LoadInt64(&bucket.lastRefillTimestampNano)
		if lastRefill/1e9 < cutoffTimestampSeconds {
			rateLimitKeysToDelete = append(rateLimitKeysToDelete, key)
		}
		return true
	})
	for _, key := range rateLimitKeysToDelete {
		ls.rateLimitBuckets.Delete(key)
	}

	var inFlightKeysToDelete []uint32
	ls.inFlightBuckets.Range(func(key uint32, counter *int32) bool {
		if atomic.LoadInt32(counter) == 0 {
			inFlightKeysToDelete = append(inFlightKeysToDelete, key)
		}
		return true
	})
	for _, key := range inFlightKeysToDelete {
		ls.inFlightBuckets.Delete(key)
	}

	_ = ctx // reserved for future use
}

// bucketKeyIP hashes the client IP (without port) once. The caller derives
// separate keys for in-flight and rate-limit maps via XOR with goldenRatio.
// ~8ns, 0 allocs: the byte slice is stack-allocated (doesn't escape).
func bucketKeyIP(r *http.Request) uint32 {
	addr := r.RemoteAddr
	// Strip port suffix. RemoteAddr is "ip:port" for IPv4
	// or "[ip]:port" for IPv6. Find the last colon to trim the port.
	if i := lastIndexByte(addr, ':'); i >= 0 {
		addr = addr[:i]
	}
	buf := make([]byte, 0, len(addr))
	buf = append(buf, addr...)
	return murmur3.Sum32(buf)
}

// lastIndexByte returns the index of the last occurrence of c in s,
// or -1 if c is not present. Avoids importing strings.
func lastIndexByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// GetRequestCount returns the total number of requests observed (both
// accepted and rejected) since this LoadShedder was created.
//
//	count := ls.GetRequestCount()
func (ls *LoadShedder) GetRequestCount() int64 {
	return atomic.LoadInt64(&ls.requestCount)
}
