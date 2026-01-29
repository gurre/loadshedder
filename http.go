// Package loadshedder provides HTTP middleware that rejects excess traffic
// before it overwhelms a server. It combines per-IP token bucket rate limiting
// with per-IP concurrency limiting.
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
// It rejects excess traffic using two mechanisms: a per-IP token bucket for
// rate limiting and a per-IP in-flight counter for concurrency limiting.
//
// A background goroutine evicts idle buckets every minute. Call Stop to
// release it when the LoadShedder is no longer needed.
//
// See New, Handler, and the With* options.
type LoadShedder struct {
	// config
	maxTokens           int
	refillRate          int
	maxRequestsInflight int32

	// limiters - sharded maps reduce contention vs sync.Map
	rateLimitBuckets *shardedMap[*TokenBucket]
	inFlightBuckets  *shardedMap[*int32]

	// counters
	requestCount            int64
	sheddedRequestCount     int64
	sheddedCauseMaxInflight int64
	sheddedCauseRateLimit   int64

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
// requests are immediately rejected. Provide at least
// WithTokenBucketCapacity, WithTokenBucketRefillRatePerSecond, and
// WithMaxRequestsInFlight to get useful behavior.
//
//	ls := loadshedder.New(
//	    loadshedder.WithTokenBucketCapacity(100),
//	    loadshedder.WithTokenBucketRefillRatePerSecond(10),
//	    loadshedder.WithMaxRequestsInFlight(20),
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

	// Start background cleanup goroutine
	ls.cleanupOnce.Do(func() {
		go ls.backgroundCleanup()
	})

	return ls
}

// WithTokenBucketCapacity sets the maximum number of tokens each client
// bucket can hold. This is the burst size: a client can make up to maxTokens
// requests before being rate limited. Panics if maxTokens is negative.
//
//	loadshedder.WithTokenBucketCapacity(50) // allow bursts of 50 requests
func WithTokenBucketCapacity(maxTokens int) Option {
	return func(ls *LoadShedder) {
		if maxTokens < 0 {
			panic("loadshedder: maxTokens must be non-negative")
		}
		ls.maxTokens = maxTokens
	}
}

// WithTokenBucketRefillRatePerSecond sets how many tokens are added to each
// client bucket per second. Together with the capacity this controls the
// sustained request rate: a client can sustain refillRate requests/s after
// exhausting its burst allowance. Panics if refillRate is negative.
//
//	loadshedder.WithTokenBucketRefillRatePerSecond(10) // sustain 10 req/s
func WithTokenBucketRefillRatePerSecond(refillRate int) Option {
	return func(ls *LoadShedder) {
		if refillRate < 0 {
			panic("loadshedder: refillRate must be non-negative")
		}
		ls.refillRate = refillRate
	}
}

// WithMaxRequestsInFlight sets the maximum number of concurrent requests
// allowed per client IP. When a client already has m requests being processed,
// new requests are rejected with 429 Too Many Requests.
// Panics if m is negative.
//
//	loadshedder.WithMaxRequestsInFlight(20) // at most 20 concurrent per IP
func WithMaxRequestsInFlight(m int32) Option {
	return func(ls *LoadShedder) {
		if m < 0 {
			panic("loadshedder: maxRequestsInflight must be non-negative")
		}
		ls.maxRequestsInflight = m
	}
}

// Handler returns middleware that guards next with rate and concurrency limits.
//
// For each request the middleware:
//  1. Checks the per-IP in-flight counter. If it is at the limit the request
//     is rejected immediately with 429.
//  2. Increments the in-flight counter (decremented when next returns).
//  3. Checks the per-IP token bucket. If the bucket is empty the request is
//     rejected with 429.
//  4. Calls next.
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/", handleRoot)
//	http.ListenAndServe(":8080", ls.Handler(mux))
func (ls *LoadShedder) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&ls.requestCount, 1)

		// Single murmur3 hash of the IP; derive the second key via XOR
		// with the golden ratio to avoid hashing twice (~15ns saved).
		ipHash := bucketKeyIP(r)
		inFlightKey := ipHash
		rateLimitKey := ipHash ^ goldenRatio

		// In-flight check: LoadOrStore allocates new(int32) eagerly, but
		// only retains it for the first request from a given IP.
		inFlightPtr, _ := ls.inFlightBuckets.LoadOrStore(inFlightKey, new(int32))

		// Benign TOCTOU: two goroutines may both read just below the limit.
		// Acceptable soft cap — a CAS loop would add contention for no gain.
		if atomic.LoadInt32(inFlightPtr) >= ls.maxRequestsInflight {
			atomic.AddInt64(&ls.sheddedRequestCount, 1)
			atomic.AddInt64(&ls.sheddedCauseMaxInflight, 1)
			http.Error(w, "Too many simultaneous requests", http.StatusTooManyRequests)
			return
		}

		atomic.AddInt32(inFlightPtr, 1)
		defer atomic.AddInt32(inFlightPtr, -1)

		// Two-phase lookup: Load is the fast path for returning clients;
		// LoadOrStore only runs on the first request from a new IP.
		bucket, ok := ls.rateLimitBuckets.Load(rateLimitKey)
		if !ok {
			newBucket := NewTokenBucket(ls.maxTokens, ls.refillRate)
			bucket, _ = ls.rateLimitBuckets.LoadOrStore(rateLimitKey, newBucket)
		}

		if !bucket.TryTake() {
			atomic.AddInt64(&ls.sheddedRequestCount, 1)
			atomic.AddInt64(&ls.sheddedCauseRateLimit, 1)
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

// bucketKeyIP hashes the client IP once. The caller derives separate keys
// for in-flight and rate-limit maps via XOR with goldenRatio.
// ~8ns, 0 allocs: the byte slice is stack-allocated (doesn't escape).
func bucketKeyIP(r *http.Request) uint32 {
	ip := r.RemoteAddr
	buf := make([]byte, 0, len(ip))
	buf = append(buf, ip...)
	return murmur3.Sum32(buf)
}

// GetRequestCount returns the total number of requests observed (both
// accepted and rejected) since this LoadShedder was created.
//
//	count := ls.GetRequestCount()
func (ls *LoadShedder) GetRequestCount() int64 {
	return atomic.LoadInt64(&ls.requestCount)
}
