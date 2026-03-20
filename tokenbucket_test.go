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

// Property: a fresh bucket with N tokens yields exactly N successful takes.
func TestTokenBucket_ExactDrain(t *testing.T) {
	for _, n := range []int{0, 1, 5, 100} {
		tb := NewTokenBucket(n, 0)
		got := 0
		for i := 0; i < n+10; i++ {
			if tb.TryTake() {
				got++
			}
		}
		if got != n {
			t.Errorf("maxTokens=%d: got %d successful takes, want %d", n, got, n)
		}
	}
}

func TestTokenBucket_ExhaustAndRefill(t *testing.T) {
	tb := NewTokenBucket(2, 2) // 2 tokens, refill 2 per second

	for i := range 2 {
		if !tb.TryTake() {
			t.Fatalf("Expected to take initial token %d", i)
		}
	}
	if tb.TryTake() {
		t.Fatal("Expected empty bucket")
	}

	time.Sleep(1100 * time.Millisecond)

	for i := range 2 {
		if !tb.TryTake() {
			t.Fatalf("Expected to take refilled token %d", i)
		}
	}
	if tb.TryTake() {
		t.Fatal("Expected empty bucket after taking refilled tokens")
	}
}

func TestTokenBucket_SubSecondElapsedDoesNotRefill(t *testing.T) {
	tb := NewTokenBucket(5, 2)

	for i := 0; i < 3; i++ {
		if !tb.TryTake() {
			t.Fatalf("Expected to take token %d", i+1)
		}
	}

	// Refill uses integer-second truncation — 0.5s yields 0 tokens.
	time.Sleep(500 * time.Millisecond)

	for i := range 2 {
		if !tb.TryTake() {
			t.Fatalf("Expected remaining token %d to still be available", i)
		}
	}
	if tb.TryTake() {
		t.Fatal("Expected bucket empty (no sub-second refill)")
	}
}

// Property: refill never exceeds maxTokens, even with high refill and long wait.
func TestTokenBucket_RefillCappedAtMax(t *testing.T) {
	maxTokens := 3
	tb := NewTokenBucket(maxTokens, 10)

	for i := 0; i < maxTokens; i++ {
		tb.TryTake()
	}

	time.Sleep(1100 * time.Millisecond)

	got := 0
	for i := 0; i < maxTokens*3; i++ {
		if tb.TryTake() {
			got++
		}
	}
	if got != maxTokens {
		t.Errorf("Expected exactly %d tokens after refill, got %d", maxTokens, got)
	}
}

// Property: with N tokens and 0 refill, exactly N of M concurrent goroutines succeed.
func TestTokenBucket_ConcurrentConservation(t *testing.T) {
	for _, n := range []int{1, 10, 100, 500} {
		tb := NewTokenBucket(n, 0)
		numGoroutines := n * 3
		var wg sync.WaitGroup
		var successful int64

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

		if successful != int64(n) {
			t.Errorf("maxTokens=%d goroutines=%d: got %d successes, want %d",
				n, numGoroutines, successful, n)
		}
	}
}

// Property: under high contention with multiple takes per goroutine,
// total successes still equal the initial capacity.
func TestTokenBucket_HighContentionConservation(t *testing.T) {
	tb := NewTokenBucket(10, 0)
	numGoroutines := 50
	var wg sync.WaitGroup
	var totalTaken int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				if tb.TryTake() {
					atomic.AddInt64(&totalTaken, 1)
				}
			}
		}()
	}
	wg.Wait()

	if totalTaken != 10 {
		t.Errorf("Expected exactly 10 successful takes, got %d", totalTaken)
	}
}

func TestTokenBucket_ZeroRefillRate(t *testing.T) {
	tb := NewTokenBucket(3, 0)

	for i := 0; i < 3; i++ {
		if !tb.TryTake() {
			t.Fatalf("Expected to take token %d", i+1)
		}
	}

	time.Sleep(1100 * time.Millisecond)

	if tb.TryTake() {
		t.Error("Expected no refill with zero refill rate")
	}
}

func TestTokenBucket_ConcurrentRefillAndTake(t *testing.T) {
	tb := NewTokenBucket(10, 5) // 10 tokens max, 5 per second refill
	duration := 2 * time.Second
	var wg sync.WaitGroup
	var totalTaken int64
	done := make(chan struct{})

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

	time.Sleep(duration)
	close(done)
	wg.Wait()

	// Initial 10 + refilled ~10 (5/s * 2s) = ~20
	expectedMin := int64(15)
	expectedMax := int64(25)

	if totalTaken < expectedMin || totalTaken > expectedMax {
		t.Errorf("Expected between %d and %d tokens over %v, got %d",
			expectedMin, expectedMax, duration, totalTaken)
	}
}
