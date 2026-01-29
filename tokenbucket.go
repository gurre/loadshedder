package loadshedder

import (
	"sync"
	"sync/atomic"
	"time"
)

// coarseNow holds a cached nanosecond timestamp updated every 100ms by a
// background goroutine. Reading this (~1ns atomic load) replaces per-call
// time.Now() (~25ns vDSO) on the hot path.
var coarseNow atomic.Int64
var coarseClockOnce sync.Once

func startCoarseClock() {
	coarseClockOnce.Do(func() {
		coarseNow.Store(time.Now().UnixNano())
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				coarseNow.Store(time.Now().UnixNano())
			}
		}()
	})
}

// TokenBucket implements the token bucket rate limiting algorithm.
// Tokens are consumed by TryTake and replenished over time at a fixed rate
// up to a maximum capacity. All operations are lock-free using atomic
// operations, making it safe for concurrent use without mutexes.
type TokenBucket struct {
	tokens                  int64
	maxTokens               int64
	refillRate              int64
	lastRefillTimestampNano int64
}

// NewTokenBucket creates a bucket that starts full with maxTokens tokens and
// refills at refillRate tokens per second, never exceeding maxTokens.
//
//	tb := loadshedder.NewTokenBucket(100, 10) // 100 tokens, 10/s refill
func NewTokenBucket(maxTokens, refillRate int) *TokenBucket {
	startCoarseClock()
	return &TokenBucket{
		tokens:                  int64(maxTokens),
		maxTokens:               int64(maxTokens),
		refillRate:              int64(refillRate),
		lastRefillTimestampNano: time.Now().UnixNano(),
	}
}

// TryTake attempts to consume one token. It first refills the bucket based on
// elapsed time, then tries to decrement the token count. Returns true if a
// token was consumed, false if the bucket was empty.
//
//	if tb.TryTake() {
//	    // request allowed
//	}
func (tb *TokenBucket) TryTake() bool {
	// Read the coarse clock (~1ns atomic load) instead of calling
	// time.Now() (~25ns). The clock is updated every 100ms which is
	// sufficient since refills operate on whole-second boundaries.
	now := coarseNow.Load()

	tb.refill(now)
	return tb.takeToken()
}

// refill adds tokens based on elapsed time since the last refill.
//
// This runs as a CAS retry loop: on contention, only one goroutine wins
// the swap and updates the timestamp; the losers retry and see the updated
// timestamp, causing them to exit early (now <= lastRefill). In practice,
// contention here is low because refills only happen when at least one
// second has passed.
func (tb *TokenBucket) refill(now int64) {
	for {
		lastRefill := atomic.LoadInt64(&tb.lastRefillTimestampNano)
		if now <= lastRefill {
			return
		}

		elapsed := now - lastRefill
		// Integer division truncates sub-second elapsed time. This means
		// tokens are added in whole-second increments — a deliberate
		// choice to keep the arithmetic simple and overflow-safe.
		elapsedSeconds := elapsed / 1e9
		if elapsedSeconds <= 0 {
			return
		}

		// Guard against int64 overflow when refillRate * elapsedSeconds
		// would exceed math.MaxInt64. In practice this requires either an
		// absurdly large refill rate or a bucket untouched for centuries.
		var refillTokens int64
		if elapsedSeconds > 0 && tb.refillRate > 0 && elapsedSeconds > (1<<63-1)/tb.refillRate {
			refillTokens = tb.maxTokens
		} else {
			refillTokens = elapsedSeconds * tb.refillRate
		}

		if refillTokens <= 0 {
			return
		}

		currentTokens := atomic.LoadInt64(&tb.tokens)
		newTokens := minInt64(currentTokens+refillTokens, tb.maxTokens)

		// Double CAS: first update tokens, then advance the timestamp.
		// If either fails, another goroutine raced us — retry from the
		// top with a fresh read of lastRefill.
		if atomic.CompareAndSwapInt64(&tb.tokens, currentTokens, newTokens) &&
			atomic.CompareAndSwapInt64(&tb.lastRefillTimestampNano, lastRefill, now) {
			return
		}
	}
}

// takeToken decrements the token count by one using atomic.AddInt64.
// Unlike a CAS retry loop, this never spins: the subtract always
// succeeds in a single instruction. If the result is negative (bucket
// was empty or raced to zero), we add back and return false. The token
// count can momentarily go negative by the number of concurrent callers,
// but correctness is preserved because every over-decrement is
// immediately reversed.
func (tb *TokenBucket) takeToken() bool {
	if atomic.AddInt64(&tb.tokens, -1) >= 0 {
		return true
	}
	atomic.AddInt64(&tb.tokens, 1)
	return false
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}
