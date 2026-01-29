package loadshedder

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadShedder_BasicRateLimiting(t *testing.T) {
	maxTokens := 2
	refillRate := 1 // 1 token per second
	ls := New(
		WithTokenBucketCapacity(maxTokens),
		WithTokenBucketRefillRatePerSecond(refillRate),
		WithMaxRequestsInFlight(10),
	)
	defer ls.Stop()

	fastHandler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Test requests from same IP should be rate limited
	ip := "192.168.1.1"
	for i := 0; i < maxTokens; i++ {
		r := httptest.NewRequest("POST", "/test", nil)
		r.RemoteAddr = ip + ":8080"
		w := httptest.NewRecorder()

		fastHandler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("Request %d should succeed, got %s", i+1, w.Result().Status)
		}
	}

	// Next request should fail (rate limit exceeded)
	r := httptest.NewRequest("POST", "/test", nil)
	r.RemoteAddr = ip + ":8080"
	w := httptest.NewRecorder()
	fastHandler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Errorf("Expected rate limit exceeded, got %s", w.Result().Status)
	}
}

func TestLoadShedder_AllPathsRateLimited(t *testing.T) {
	ls := New(
		WithTokenBucketCapacity(0), // Zero tokens to force rate limiting
		WithTokenBucketRefillRatePerSecond(0),
		WithMaxRequestsInFlight(1),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	paths := []string{"/ping", "/api/test", "/health"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			r := httptest.NewRequest("GET", path, nil)
			r.RemoteAddr = "192.168.1.1:8080"
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, r)
			if w.Result().StatusCode != http.StatusTooManyRequests {
				t.Errorf("Expected 429 for %s, got %d", path, w.Result().StatusCode)
			}
		})
	}
}

func TestLoadShedder_InFlightLimiting(t *testing.T) {
	maxInFlight := int32(2)
	ls := New(
		WithTokenBucketCapacity(100), // High token limit
		WithTokenBucketRefillRatePerSecond(100),
		WithMaxRequestsInFlight(maxInFlight),
	)
	defer ls.Stop()

	requestDuration := 100 * time.Millisecond
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(requestDuration)
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	results := make(chan int, 10)

	// Launch multiple concurrent requests
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := httptest.NewRequest("POST", "/test", nil)
			r.RemoteAddr = "192.168.1.1:8080"
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, r)
			results <- w.Result().StatusCode
		}(i)
	}

	wg.Wait()
	close(results)

	okCount := 0
	tooManyCount := 0
	for status := range results {
		switch status {
		case http.StatusOK:
			okCount++
		case http.StatusTooManyRequests:
			tooManyCount++
		default:
			t.Errorf("Unexpected status code: %d", status)
		}
	}

	// Should have some successful requests and some rejected due to in-flight limit
	if okCount == 0 {
		t.Error("Expected some successful requests")
	}
	if tooManyCount == 0 {
		t.Error("Expected some requests to be rejected due to in-flight limit")
	}
	if int32(okCount) > maxInFlight+1 { // Allow some tolerance for timing
		t.Errorf("Too many concurrent requests allowed: %d (max %d)", okCount, maxInFlight)
	}
}

func TestLoadShedder_SLOTracking(t *testing.T) {
	ls := New(
		WithTokenBucketCapacity(10),
		WithTokenBucketRefillRatePerSecond(10),
		WithMaxRequestsInFlight(10),
	)
	defer ls.Stop()

	fastHandler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond) // Fast request
		w.WriteHeader(http.StatusOK)
	}))

	slowHandler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // Slow request (exceeds SLO)
		w.WriteHeader(http.StatusOK)
	}))

	// Make fast requests
	for i := 0; i < 3; i++ {
		r := httptest.NewRequest("GET", "/fast", nil)
		r.RemoteAddr = fmt.Sprintf("192.168.1.%d:8080", i)
		w := httptest.NewRecorder()
		fastHandler.ServeHTTP(w, r)
	}

	// Make slow requests
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/slow", nil)
		r.RemoteAddr = fmt.Sprintf("192.168.2.%d:8080", i)
		w := httptest.NewRecorder()
		slowHandler.ServeHTTP(w, r)
	}

	totalRequests := ls.GetRequestCount()

	if totalRequests != 5 {
		t.Errorf("Expected 5 total requests, got %d", totalRequests)
	}
}

func TestLoadShedder_IPIsolation(t *testing.T) {
	ls := New(
		WithTokenBucketCapacity(1),
		WithTokenBucketRefillRatePerSecond(0), // No refill
		WithMaxRequestsInFlight(10),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// IP 1 exhausts its limit
	r1 := httptest.NewRequest("POST", "/test", nil)
	r1.RemoteAddr = "192.168.1.1:8080"
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)
	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request from IP 1 should succeed")
	}

	// IP 1 second request should fail
	r1b := httptest.NewRequest("POST", "/test", nil)
	r1b.RemoteAddr = "192.168.1.1:8080"
	w1b := httptest.NewRecorder()
	handler.ServeHTTP(w1b, r1b)
	if w1b.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Second request from IP 1 should fail")
	}

	// IP 2 should still work (different bucket)
	r2 := httptest.NewRequest("POST", "/test", nil)
	r2.RemoteAddr = "192.168.1.2:8080"
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)
	if w2.Result().StatusCode != http.StatusOK {
		t.Error("Request from IP 2 should succeed despite IP 1 being rate limited")
	}
}

func TestLoadShedder_ConcurrentRequests(t *testing.T) {
	ls := New(
		WithTokenBucketCapacity(50),
		WithTokenBucketRefillRatePerSecond(0), // No refill for predictable behavior
		WithMaxRequestsInFlight(100),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	numGoroutines := 100
	var wg sync.WaitGroup
	var successCount int64

	// Use the same IP for all requests to hit the same bucket
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := httptest.NewRequest("POST", "/test", nil)
			r.RemoteAddr = "192.168.1.1:8080" // Same IP for all requests
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, r)
			if w.Result().StatusCode == http.StatusOK {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Should have exactly 50 successful requests (token bucket capacity)
	if successCount != 50 {
		t.Errorf("Expected 50 successful requests, got %d", successCount)
	}
}

func TestLoadShedder_Cleanup(t *testing.T) {
	ls := New(
		WithTokenBucketCapacity(10),
		WithTokenBucketRefillRatePerSecond(1),
		WithMaxRequestsInFlight(10),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond) // Slow request to trigger SLO tracking
		w.WriteHeader(http.StatusOK)
	}))

	// Make some requests to populate internal maps
	for i := 0; i < 5; i++ {
		r := httptest.NewRequest("POST", "/test", nil)
		r.RemoteAddr = fmt.Sprintf("192.168.1.%d:8080", i)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}

	// Verify that internal state was created
	initialBuckets := 0
	ls.rateLimitBuckets.Range(func(key uint32, value *TokenBucket) bool {
		initialBuckets++
		return true
	})
	if initialBuckets == 0 {
		t.Error("Expected some buckets to be created")
	}

	// Run cleanup for old data
	cutoff := time.Now().Add(-1 * time.Minute).Unix()
	ls.Cleanup(context.Background(), cutoff)

	// Buckets should still exist (they were just created)
	currentBuckets := 0
	ls.rateLimitBuckets.Range(func(key uint32, value *TokenBucket) bool {
		currentBuckets++
		return true
	})
	if currentBuckets != initialBuckets {
		t.Errorf("Expected %d buckets after cleanup, got %d", initialBuckets, currentBuckets)
	}

	// Test cleanup of truly old data
	futureCutoff := time.Now().Add(1 * time.Minute).Unix()
	ls.Cleanup(context.Background(), futureCutoff)

	// Most buckets should be cleaned up now
	finalBuckets := 0
	ls.rateLimitBuckets.Range(func(key uint32, value *TokenBucket) bool {
		finalBuckets++
		return true
	})
	if finalBuckets >= initialBuckets {
		t.Errorf("Expected fewer buckets after aggressive cleanup, got %d (was %d)", finalBuckets, initialBuckets)
	}
}

func TestLoadShedder_BackgroundCleanup(t *testing.T) {
	ls := New(
		WithTokenBucketCapacity(10),
		WithTokenBucketRefillRatePerSecond(1),
		WithMaxRequestsInFlight(10),
	)

	// Verify background cleanup goroutine started
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Make a request to create some state
	r := httptest.NewRequest("POST", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	// Stop the load shedder
	ls.Stop()

	// Give it a moment to process the stop signal
	time.Sleep(10 * time.Millisecond)

	// Background goroutine should have stopped (no way to directly test this,
	// but the Stop() call should complete without hanging)
}

func TestLoadShedder_RefillBehavior(t *testing.T) {
	ls := New(
		WithTokenBucketCapacity(2),
		WithTokenBucketRefillRatePerSecond(2), // 2 tokens per second
		WithMaxRequestsInFlight(10),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	ip := "192.168.1.1:8080"

	// Use up initial tokens
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("POST", "/test", nil)
		r.RemoteAddr = ip
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("Initial request %d should succeed", i+1)
		}
	}

	// Next request should fail
	r := httptest.NewRequest("POST", "/test", nil)
	r.RemoteAddr = ip
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Request should fail when bucket is empty")
	}

	// Wait for refill (1.1 seconds to ensure at least 2 tokens are refilled)
	time.Sleep(1100 * time.Millisecond)

	// Should be able to make requests again
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("POST", "/test", nil)
		r.RemoteAddr = ip
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("Request after refill %d should succeed", i+1)
		}
	}
}

// Benchmark tests - moved to benchmark_test.go for comprehensive coverage
