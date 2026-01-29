package loadshedder

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucket_TryTake(t *testing.T) {
	tests := []struct {
		name       string
		maxTokens  int
		refillRate int
		want       bool
	}{
		{"zero tokens", 0, 0, false},
		{"one token", 1, 0, true},
		{"one token with refill", 1, 1, true},
		{"multiple tokens", 5, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := NewTokenBucket(tt.maxTokens, tt.refillRate)
			if got := tb.TryTake(); got != tt.want {
				t.Errorf("TokenBucket.TryTake() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTokenBucket_InitialState(t *testing.T) {
	maxTokens := 10
	refillRate := 5
	tb := NewTokenBucket(maxTokens, refillRate)

	// Check initial values are set correctly
	if atomic.LoadInt64(&tb.tokens) != int64(maxTokens) {
		t.Errorf("Expected initial tokens to be %d, got %d", maxTokens, atomic.LoadInt64(&tb.tokens))
	}
	if tb.maxTokens != int64(maxTokens) {
		t.Errorf("Expected maxTokens to be %d, got %d", maxTokens, tb.maxTokens)
	}
	if tb.refillRate != int64(refillRate) {
		t.Errorf("Expected refillRate to be %d, got %d", refillRate, tb.refillRate)
	}
	if atomic.LoadInt64(&tb.lastRefillTimestampNano) == 0 {
		t.Error("Expected lastRefillTimestampNano to be set")
	}
}

func TestTokenBucket_ExhaustAndRefill(t *testing.T) {
	tb := NewTokenBucket(2, 2) // 2 tokens, refill 2 per second

	// Take all initial tokens
	if !tb.TryTake() {
		t.Error("Expected to take first token")
	}
	if !tb.TryTake() {
		t.Error("Expected to take second token")
	}

	// Should not be able to take more tokens immediately
	if tb.TryTake() {
		t.Error("Expected to not take token when bucket is empty")
	}

	// Wait for refill (1.1 seconds to ensure refill happens)
	time.Sleep(1100 * time.Millisecond)

	// Should be able to take tokens after refill
	if !tb.TryTake() {
		t.Error("Expected to take token after refill")
	}
	if !tb.TryTake() {
		t.Error("Expected to take second token after refill")
	}

	// Should not be able to take more than max
	if tb.TryTake() {
		t.Error("Expected to not take token when bucket is empty after refill")
	}
}

func TestTokenBucket_PartialRefill(t *testing.T) {
	tb := NewTokenBucket(5, 2) // 5 tokens max, refill 2 per second

	// Take 3 tokens, leaving 2
	for i := 0; i < 3; i++ {
		if !tb.TryTake() {
			t.Errorf("Expected to take token %d", i+1)
		}
	}

	// Wait for 0.5 seconds (should not refill any tokens yet)
	time.Sleep(500 * time.Millisecond)

	// Should still have only 2 tokens
	if !tb.TryTake() {
		t.Error("Expected to take token before refill")
	}
	if !tb.TryTake() {
		t.Error("Expected to take second token before refill")
	}
	if tb.TryTake() {
		t.Error("Expected to not take token when bucket should be empty")
	}
}

func TestTokenBucket_MaxCapacityLimit(t *testing.T) {
	tb := NewTokenBucket(3, 10) // 3 tokens max, very fast refill

	// Take all tokens
	for i := 0; i < 3; i++ {
		if !tb.TryTake() {
			t.Errorf("Expected to take token %d", i+1)
		}
	}

	// Wait for more than enough time to refill way beyond capacity
	time.Sleep(2 * time.Second)

	// Should only be able to take up to max capacity
	successfulTakes := 0
	for i := 0; i < 10; i++ { // Try to take more than max
		if tb.TryTake() {
			successfulTakes++
		}
	}

	if successfulTakes != 3 {
		t.Errorf("Expected to take exactly 3 tokens (max capacity), got %d", successfulTakes)
	}
}

func TestTokenBucket_ConcurrentAccess(t *testing.T) {
	tb := NewTokenBucket(100, 0) // 100 tokens, no refill
	numGoroutines := 200
	var wg sync.WaitGroup
	var successful int64

	// Launch concurrent token requests
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tb.TryTake() {
				atomic.AddInt64(&successful, 1)
			}
		}()
	}

	wg.Wait()

	// Should have exactly 100 successful takes (the initial token count)
	if successful != 100 {
		t.Errorf("Expected exactly 100 successful takes, got %d", successful)
	}

	// Bucket should now be empty
	if tb.TryTake() {
		t.Error("Expected bucket to be empty after concurrent access")
	}
}

func TestTokenBucket_ConcurrentRefillAndTake(t *testing.T) {
	tb := NewTokenBucket(10, 5) // 10 tokens max, 5 per second refill
	duration := 2 * time.Second
	var wg sync.WaitGroup
	var totalTaken int64
	done := make(chan struct{})

	// Goroutine that continuously tries to take tokens
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if tb.TryTake() {
					atomic.AddInt64(&totalTaken, 1)
				}
			case <-done:
				return
			}
		}
	}()

	// Let it run for the specified duration
	time.Sleep(duration)
	close(done)
	wg.Wait()

	// We should have taken approximately:
	// Initial: 10 tokens
	// Refilled: 5 tokens/second * 2 seconds = 10 tokens
	// Total expected: ~20 tokens (allowing some variance for timing)
	expectedMin := int64(15) // Allow some margin for timing variations
	expectedMax := int64(25)

	if totalTaken < expectedMin || totalTaken > expectedMax {
		t.Errorf("Expected to take between %d and %d tokens over %v, got %d",
			expectedMin, expectedMax, duration, totalTaken)
	}
}

func TestTokenBucket_ZeroRefillRate(t *testing.T) {
	tb := NewTokenBucket(3, 0) // 3 tokens, no refill

	// Take all tokens
	for i := 0; i < 3; i++ {
		if !tb.TryTake() {
			t.Errorf("Expected to take token %d", i+1)
		}
	}

	// Wait and verify no refill happens
	time.Sleep(1 * time.Second)

	if tb.TryTake() {
		t.Error("Expected no refill with zero refill rate")
	}
}

func TestTokenBucket_HighContentionScenario(t *testing.T) {
	tb := NewTokenBucket(10, 0) // 10 tokens max, no refill for predictable testing
	numGoroutines := 50         // Goroutines competing for tokens
	var wg sync.WaitGroup
	var totalTaken int64

	// High contention: many goroutines competing for tokens
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine tries to take tokens multiple times
			for j := 0; j < 5; j++ {
				if tb.TryTake() {
					atomic.AddInt64(&totalTaken, 1)
				}
				// Small delay to increase contention
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// With no refill, we should get exactly 10 successful takes regardless of contention
	// This tests that the atomic operations work correctly under high contention
	if totalTaken != 10 {
		t.Errorf("Expected exactly 10 token consumption (bucket capacity) under high contention, got %d", totalTaken)
	}

	// Verify bucket is truly empty
	if tb.TryTake() {
		t.Error("Expected bucket to be empty after high contention test")
	}
}

// Benchmark tests
func BenchmarkTokenBucket_TryTake_Success(b *testing.B) {
	tb := NewTokenBucket(b.N*2, 0) // Enough tokens for all operations

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.TryTake()
	}
}

func BenchmarkTokenBucket_TryTake_Failure(b *testing.B) {
	tb := NewTokenBucket(0, 0) // No tokens

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.TryTake()
	}
}

func BenchmarkTokenBucket_ConcurrentAccess(b *testing.B) {
	tb := NewTokenBucket(1000000, 0) // Large bucket for benchmark

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = tb.TryTake()
		}
	})
}
