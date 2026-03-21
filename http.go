// Package loadshedder provides HTTP middleware that rejects excess traffic
// before it overwhelms a server. It combines server-wide and per-IP burst/rate
// limiting with server-wide and per-IP concurrency limiting.
//
// Create a LoadShedder with New, wrap your handler with Handler, and call
// Stop on shutdown.
package loadshedder

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"net/http"
	"net/netip"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// goldenRatio is used to derive a second hash key from a single murmur3
// hash, avoiding the cost of hashing the IP address twice.
const goldenRatio = 0x9e3779b9

// murmur3Seed is a random seed chosen at process start to prevent
// hash-flooding attacks from predicting bucket placement.
var murmur3Seed uint32

func init() {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("loadshedder: failed to read crypto/rand: " + err.Error())
	}
	murmur3Seed = binary.LittleEndian.Uint32(buf[:])
}

// Config configures a LoadShedder. At least one of PerIP or Server must be
// non-nil. Each non-nil config must have at least one positive limit.
//
// Validation rules:
//   - At least one of PerIP or Server must be non-nil.
//   - Each non-nil config must have at least one positive Burst, Rate, or MaxInFlight.
//   - All values must be non-negative.
//   - MaxInFlight must not exceed math.MaxInt32 (internal atomics use int32).
//   - When Burst is set without Rate (or vice versa), New defaults the missing
//     value to the other: PerIPConfig{Rate: 10} becomes {Burst: 10, Rate: 10}.
//
// Use Validate to check for errors, or pass directly to New which panics on
// invalid config.
type Config struct {
	PerIP  *PerIPConfig  // nil = per-IP disabled
	Server *ServerConfig // nil = no server-wide limits
}

// PerIPConfig controls per-client rate and concurrency limits.
// At least one of Burst, Rate, or MaxInFlight must be positive.
type PerIPConfig struct {
	Burst       int    // max tokens per client bucket
	Rate        int    // tokens/sec refill per client (defaults to Burst if zero)
	MaxInFlight int    // max concurrent per client (0 = no concurrency limit); must be <= math.MaxInt32
	ProxyHeader string // header for real client IP (case-insensitive, internally canonicalized; empty = use RemoteAddr)
}

// ServerConfig controls server-wide rate and concurrency limits.
// At least one of Burst, Rate, or MaxInFlight must be positive.
type ServerConfig struct {
	Burst       int // max tokens server-wide
	Rate        int // tokens/sec refill server-wide (defaults to Burst if zero)
	MaxInFlight int // max concurrent server-wide (0 = no concurrency limit); must be <= math.MaxInt32
}

// Validate checks that the Config is valid. Returns an error describing the
// first problem found. Called by New, which panics on error.
func (c Config) Validate() error {
	if c.PerIP == nil && c.Server == nil {
		return errors.New("loadshedder: at least one of PerIP or Server must be non-nil")
	}

	if c.PerIP != nil {
		if c.PerIP.Burst < 0 {
			return errors.New("loadshedder: PerIP.Burst must be non-negative")
		}
		if c.PerIP.Rate < 0 {
			return errors.New("loadshedder: PerIP.Rate must be non-negative")
		}
		if c.PerIP.MaxInFlight < 0 {
			return errors.New("loadshedder: PerIP.MaxInFlight must be non-negative")
		}
		if c.PerIP.MaxInFlight > math.MaxInt32 {
			return errors.New("loadshedder: PerIP.MaxInFlight exceeds max int32")
		}
		if c.PerIP.Burst == 0 && c.PerIP.Rate == 0 && c.PerIP.MaxInFlight == 0 {
			return errors.New("loadshedder: PerIP requires at least one of Burst, Rate, or MaxInFlight")
		}
	}

	if c.Server != nil {
		if c.Server.Burst < 0 {
			return errors.New("loadshedder: Server.Burst must be non-negative")
		}
		if c.Server.Rate < 0 {
			return errors.New("loadshedder: Server.Rate must be non-negative")
		}
		if c.Server.MaxInFlight < 0 {
			return errors.New("loadshedder: Server.MaxInFlight must be non-negative")
		}
		if c.Server.MaxInFlight > math.MaxInt32 {
			return errors.New("loadshedder: Server.MaxInFlight exceeds max int32")
		}
		if c.Server.Burst == 0 && c.Server.Rate == 0 && c.Server.MaxInFlight == 0 {
			return errors.New("loadshedder: Server requires at least one of Burst, Rate, or MaxInFlight")
		}
	}

	return nil
}

// LoadShedder is an HTTP middleware that protects services from overload.
// It rejects excess traffic using four mechanisms: a server-wide burst/rate
// limiter for rate limiting, a server-wide in-flight counter for
// concurrency limiting, a per-IP burst/rate limiter for rate limiting, and a
// per-IP in-flight counter for concurrency limiting.
//
// A background goroutine evicts idle buckets every minute. Call Stop to
// release it when the LoadShedder is no longer needed.
type LoadShedder struct {
	// per-IP config
	perIPBurst           int
	perIPRate            int
	perIPMaxInFlight     int32
	perIPRateEnabled     bool // computed: PerIP != nil && (Burst > 0 || Rate > 0)
	perIPInFlightEnabled bool // computed: PerIP != nil && MaxInFlight > 0
	trustedProxyHeader   string

	// server-wide config
	serverBurst       int
	serverRate        int
	serverMaxInFlight int32

	// server-wide limiters
	serverRateBucket *TokenBucket // nil when not configured
	serverInflight   int32        // single atomic counter

	// per-IP limiters — sharded maps reduce contention vs sync.Map
	rateLimitBuckets *shardedMap[*TokenBucket]
	inFlightBuckets  *shardedMap[*int32]

	// requestCount is on its own cache line to avoid false sharing with
	// the shedded counters — it is incremented on every request while the
	// shedded counters are only incremented on rejection.
	requestCount int64
	_padReq      [56]byte // 8 + 56 = 64 bytes (one cache line)

	// shedded counters — warm path, only updated on rejection
	sheddedRequestCount        int64
	sheddedCausePerIPInFlight  int64
	sheddedCausePerIPRate      int64
	sheddedCauseServerInFlight int64
	sheddedCauseServerRate     int64

	// cleanup management
	cleanupOnce sync.Once
	stopOnce    sync.Once
	stopCleanup chan struct{}
}

// New creates a LoadShedder configured by cfg.
// It starts a background goroutine that periodically evicts idle buckets.
// Call Stop when the LoadShedder is no longer needed.
//
// Panics if cfg.Validate() returns an error.
//
// When rate is set without burst (or vice versa), New defaults the missing
// value: burst defaults to rate, and rate defaults to burst. This ensures
// that setting only one of the two still produces a usable token bucket.
// The same defaulting applies to server-wide burst and rate.
//
//	ls := loadshedder.New(loadshedder.Config{
//	    PerIP: &loadshedder.PerIPConfig{
//	        Burst: 100, Rate: 10, MaxInFlight: 20,
//	    },
//	    Server: &loadshedder.ServerConfig{
//	        Burst: 1000, Rate: 100, MaxInFlight: 200,
//	    },
//	})
//	defer ls.Stop()
func New(cfg Config) *LoadShedder {
	if err := cfg.Validate(); err != nil {
		panic(err.Error())
	}

	ls := &LoadShedder{
		rateLimitBuckets: newShardedMap[*TokenBucket](),
		inFlightBuckets:  newShardedMap[*int32](),
		stopCleanup:      make(chan struct{}),
	}

	if cfg.PerIP != nil {
		ls.perIPBurst = cfg.PerIP.Burst
		ls.perIPRate = cfg.PerIP.Rate
		ls.perIPMaxInFlight = int32(cfg.PerIP.MaxInFlight)

		// Default burst to rate (and vice versa) so that setting only one
		// produces a usable token bucket instead of a silent reject-all.
		if ls.perIPRate > 0 && ls.perIPBurst == 0 {
			ls.perIPBurst = ls.perIPRate
		}
		if ls.perIPBurst > 0 && ls.perIPRate == 0 {
			ls.perIPRate = ls.perIPBurst
		}

		ls.perIPRateEnabled = ls.perIPBurst > 0 || ls.perIPRate > 0
		ls.perIPInFlightEnabled = ls.perIPMaxInFlight > 0

		if cfg.PerIP.ProxyHeader != "" {
			ls.trustedProxyHeader = textproto.CanonicalMIMEHeaderKey(cfg.PerIP.ProxyHeader)
		}
	}

	if cfg.Server != nil {
		ls.serverBurst = cfg.Server.Burst
		ls.serverRate = cfg.Server.Rate
		ls.serverMaxInFlight = int32(cfg.Server.MaxInFlight)

		// Default burst to rate (and vice versa)
		if ls.serverRate > 0 && ls.serverBurst == 0 {
			ls.serverBurst = ls.serverRate
		}
		if ls.serverBurst > 0 && ls.serverRate == 0 {
			ls.serverRate = ls.serverBurst
		}

		if ls.serverBurst > 0 {
			ls.serverRateBucket = NewTokenBucket(ls.serverBurst, ls.serverRate)
		}
	}

	// Start background cleanup goroutine only when per-IP maps will
	// receive insertions. Server-only configs have no per-IP buckets
	// to evict, so the goroutine is unnecessary.
	if ls.perIPRateEnabled || ls.perIPInFlightEnabled {
		ls.cleanupOnce.Do(func() {
			go ls.backgroundCleanup()
		})
	}

	return ls
}

// Handler returns middleware that guards next with rate and concurrency limits.
//
// For each request the middleware checks in order:
//  1. Server-wide in-flight concurrency (if configured).
//  2. Server-wide rate limit (if configured).
//  3. Per-IP in-flight concurrency (if configured).
//  4. Per-IP rate limit (if configured).
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
		// Increment first, then validate. This eliminates the TOCTOU
		// window where GOMAXPROCS goroutines could all pass a Load check
		// simultaneously. If over the limit, revert and reject.
		if ls.serverMaxInFlight > 0 {
			if atomic.AddInt32(&ls.serverInflight, 1) > ls.serverMaxInFlight {
				atomic.AddInt32(&ls.serverInflight, -1)
				atomic.AddInt64(&ls.sheddedRequestCount, 1)
				atomic.AddInt64(&ls.sheddedCauseServerInFlight, 1)
				http.Error(w, "Too many simultaneous requests", http.StatusTooManyRequests)
				return
			}
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

		if ls.perIPRateEnabled || ls.perIPInFlightEnabled {
			// Single murmur3 hash of the IP; derive the second key via XOR
			// with the golden ratio to avoid hashing twice (~15ns saved).
			ipHash := bucketKeyIP(r, ls.trustedProxyHeader)
			inFlightKey := ipHash
			rateLimitKey := ipHash ^ goldenRatio

			// --- Per-IP in-flight check ---
			if ls.perIPInFlightEnabled {
				// Two-phase lookup: Load is the fast path for returning clients;
				// LoadOrStore only runs on the first request from a new IP.
				inFlightPtr, ok := ls.inFlightBuckets.Load(inFlightKey)
				if !ok {
					inFlightPtr, _ = ls.inFlightBuckets.LoadOrStore(inFlightKey, new(int32))
				}

				// Increment first, then validate — eliminates the TOCTOU window.
				if atomic.AddInt32(inFlightPtr, 1) > ls.perIPMaxInFlight {
					atomic.AddInt32(inFlightPtr, -1)
					atomic.AddInt64(&ls.sheddedRequestCount, 1)
					atomic.AddInt64(&ls.sheddedCausePerIPInFlight, 1)
					http.Error(w, "Too many simultaneous requests", http.StatusTooManyRequests)
					return
				}
				defer atomic.AddInt32(inFlightPtr, -1)
			}

			// --- Per-IP rate limit check ---
			if ls.perIPRateEnabled {
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
			}
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
	// Collect candidate keys under read locks, then re-validate under
	// write locks via DeleteIf. This prevents deleting a bucket that was
	// refreshed by a concurrent request between the Range and the Delete.
	var rateLimitKeysToDelete []uint32
	ls.rateLimitBuckets.Range(func(key uint32, bucket *TokenBucket) bool {
		lastRefill := atomic.LoadInt64(&bucket.lastRefillTimestampNano)
		if lastRefill/1e9 < cutoffTimestampSeconds {
			rateLimitKeysToDelete = append(rateLimitKeysToDelete, key)
		}
		return true
	})
	for _, key := range rateLimitKeysToDelete {
		ls.rateLimitBuckets.DeleteIf(key, func(bucket *TokenBucket) bool {
			return atomic.LoadInt64(&bucket.lastRefillTimestampNano)/1e9 < cutoffTimestampSeconds
		})
	}

	var inFlightKeysToDelete []uint32
	ls.inFlightBuckets.Range(func(key uint32, counter *int32) bool {
		if atomic.LoadInt32(counter) == 0 {
			inFlightKeysToDelete = append(inFlightKeysToDelete, key)
		}
		return true
	})
	for _, key := range inFlightKeysToDelete {
		ls.inFlightBuckets.DeleteIf(key, func(counter *int32) bool {
			return atomic.LoadInt32(counter) == 0
		})
	}

	_ = ctx // reserved for future use
}

// bucketKeyIP hashes the client IP (without port) once. The caller derives
// separate keys for in-flight and rate-limit maps via XOR with goldenRatio.
//
// When trustedProxyHeader is set and the header is present, the rightmost
// comma-separated value from the last header entry is used instead of
// RemoteAddr. The rightmost value is the IP added by the nearest trusted
// proxy, making it the only value an attacker cannot forge.
//
// Header-sourced IPs are parsed with netip.ParseAddr to validate and
// normalize them (e.g. IPv6 forms). If parsing fails — including when
// the header is present but contains only whitespace, commas, or garbage
// — the function falls back to RemoteAddr. In proxy deployments this
// means the proxy's IP is used as the rate-limit key for that request,
// which is conservative (shared bucket) but not client-specific.
// The RemoteAddr path skips ParseAddr because Go's net/http server
// already produces canonical IP strings from the TCP stack.
//
// On non-IP transports (unix sockets), RemoteAddr is not a valid IP. All
// such connections hash to the same bucket — use Config with PerIP: nil
// for these transports.
func bucketKeyIP(r *http.Request, trustedProxyHeader string) uint32 {
	addr := ""
	if trustedProxyHeader != "" {
		// Direct map lookup on pre-canonicalized key; avoids Header.Get overhead.
		if values := r.Header[trustedProxyHeader]; len(values) > 0 {
			hv := values[len(values)-1] // last header entry
			// Scan backwards for the last comma to find the rightmost value.
			// O(n) but typical XFF headers are 20-40 bytes.
			i := len(hv) - 1
			for i >= 0 && hv[i] != ',' {
				i--
			}
			addr = hv[i+1:] // everything after the last comma (or full string)
			// Trim spaces
			start, end := 0, len(addr)
			for start < end && addr[start] == ' ' {
				start++
			}
			for end > start && addr[end-1] == ' ' {
				end--
			}
			addr = addr[start:end]
		}
	}

	if addr != "" {
		// Strip brackets around IPv6 (e.g. "[::1]" → "::1")
		if len(addr) > 1 && addr[0] == '[' && addr[len(addr)-1] == ']' {
			addr = addr[1 : len(addr)-1]
		}
		// Validate and normalize: attacker-controlled header values need
		// parsing to prevent bypass via alternate IP representations.
		if parsed, err := netip.ParseAddr(addr); err == nil {
			addr = parsed.String()
		} else {
			addr = "" // invalid IP, fall back to RemoteAddr
		}
	}

	if addr == "" {
		addr = r.RemoteAddr
		// Strip port suffix. RemoteAddr is "ip:port" for IPv4
		// or "[ip]:port" for IPv6. Find the last colon to trim the port.
		if i := strings.LastIndexByte(addr, ':'); i >= 0 {
			addr = addr[:i]
		}
		// Strip brackets around IPv6
		if len(addr) > 1 && addr[0] == '[' && addr[len(addr)-1] == ']' {
			addr = addr[1 : len(addr)-1]
		}
		// RemoteAddr from Go's net/http is already canonical for TCP
		// connections (no leading zeros, standard IPv6 compression), so
		// we skip netip.ParseAddr here to avoid an allocation on the
		// hot path. Non-IP values (unix sockets) are hashed as-is.
	}

	return murmur3Sum32(addr)
}

// murmur3Sum32 computes MurmurHash3 (32-bit) without unsafe pointer
// arithmetic, so it passes Go's checkptr validation under -race.
//
// The 32-bit hash space means birthday collisions are expected around
// ~65,000 distinct IPs. Two IPs sharing a hash share a rate-limit bucket,
// which is slightly unfair but not exploitable without knowing the seed.
func murmur3Sum32(s string) uint32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
	)

	h1 := murmur3Seed
	nblocks := len(s) / 4
	for i := range nblocks {
		j := i * 4
		k1 := uint32(s[j]) | uint32(s[j+1])<<8 | uint32(s[j+2])<<16 | uint32(s[j+3])<<24
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xe6546b64
	}

	tail := s[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(len(s))
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16
	return h1
}

// GetRequestCount returns the total number of requests observed (both
// accepted and rejected) since this LoadShedder was created.
//
//	count := ls.GetRequestCount()
func (ls *LoadShedder) GetRequestCount() int64 {
	return atomic.LoadInt64(&ls.requestCount)
}
