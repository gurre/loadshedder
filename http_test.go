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
	burst := 2
	ratePerSecond := 1
	ls := New(
		WithPerIPBurst(burst),
		WithPerIPRatePerSecond(ratePerSecond),
		WithPerIPMaxInFlight(10),
	)
	defer ls.Stop()

	fastHandler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Test requests from same IP should be rate limited
	ip := "192.168.1.1"
	for i := 0; i < burst; i++ {
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

func TestLoadShedder_ZeroConfigRejectsAll(t *testing.T) {
	// Without any options, all per-IP limits are zero → reject everything.
	ls := New()
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("Handler should never be called with zero config")
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Errorf("Expected 429 with zero config, got %d", w.Result().StatusCode)
	}
}

func TestLoadShedder_AllPathsRateLimited(t *testing.T) {
	ls := New(
		WithPerIPBurst(0), // Zero tokens to force rate limiting
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1),
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
		WithPerIPBurst(100),
		WithPerIPRatePerSecond(100),
		WithPerIPMaxInFlight(maxInFlight),
	)
	defer ls.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	const totalRequests = 5
	var wg sync.WaitGroup
	results := make([]int, totalRequests)

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r := httptest.NewRequest("POST", "/test", nil)
			r.RemoteAddr = "192.168.1.1:8080"
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			results[idx] = w.Result().StatusCode
		}(i)
	}

	// Wait for maxInFlight requests to enter the handler
	for i := 0; i < int(maxInFlight); i++ {
		<-started
	}

	// Give rejected requests time to complete
	time.Sleep(50 * time.Millisecond)

	// Release all blocked handlers
	close(release)
	go func() {
		for range started {
		}
	}()
	wg.Wait()
	close(started)

	var okCount, rejectedCount int
	for _, code := range results {
		switch code {
		case http.StatusOK:
			okCount++
		case http.StatusTooManyRequests:
			rejectedCount++
		default:
			t.Errorf("Unexpected status code: %d", code)
		}
	}

	if okCount != int(maxInFlight) {
		t.Errorf("Expected %d OK responses, got %d", maxInFlight, okCount)
	}
	if rejectedCount != totalRequests-int(maxInFlight) {
		t.Errorf("Expected %d rejected responses, got %d", totalRequests-int(maxInFlight), rejectedCount)
	}
}

func TestLoadShedder_RequestCount(t *testing.T) {
	ls := New(
		WithPerIPBurst(3),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(100),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 3 accepted + 2 rejected = 5 total observed
	for i := 0; i < 5; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}

	if got := ls.GetRequestCount(); got != 5 {
		t.Errorf("Expected 5 total requests (accepted + rejected), got %d", got)
	}
}

func TestLoadShedder_IPIsolation(t *testing.T) {
	ls := New(
		WithPerIPBurst(1),
		WithPerIPRatePerSecond(0), // No refill
		WithPerIPMaxInFlight(10),
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

func TestLoadShedder_SameIPDifferentPortsShareBucket(t *testing.T) {
	ls := New(
		WithPerIPBurst(2),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(10),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Two requests from same IP but different source ports share a bucket.
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "192.168.1.1:5000"
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "192.168.1.1:6000"
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	// Both succeed (burst=2)
	if w1.Result().StatusCode != http.StatusOK || w2.Result().StatusCode != http.StatusOK {
		t.Fatal("First two requests from same IP should succeed")
	}

	// Third from yet another port — rejected (bucket exhausted)
	r3 := httptest.NewRequest("GET", "/", nil)
	r3.RemoteAddr = "192.168.1.1:7000"
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, r3)
	if w3.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Third request from same IP (different port) should be rejected")
	}
}

func TestLoadShedder_ConcurrentRequests(t *testing.T) {
	ls := New(
		WithPerIPBurst(50),
		WithPerIPRatePerSecond(0), // No refill for predictable behavior
		WithPerIPMaxInFlight(100),
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
		WithPerIPBurst(10),
		WithPerIPRatePerSecond(1),
		WithPerIPMaxInFlight(10),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

func TestLoadShedder_StopIsIdempotent(t *testing.T) {
	ls := New(
		WithPerIPBurst(10),
		WithPerIPRatePerSecond(1),
		WithPerIPMaxInFlight(10),
	)

	// Stop must not panic when called multiple times.
	ls.Stop()
	ls.Stop()
	ls.Stop()
}

func TestLoadShedder_RefillBehavior(t *testing.T) {
	ls := New(
		WithPerIPBurst(2),
		WithPerIPRatePerSecond(2), // 2 tokens per second
		WithPerIPMaxInFlight(10),
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

func TestLoadShedder_ServerRateLimiting(t *testing.T) {
	serverCapacity := 5
	ls := New(
		WithPerIPBurst(100),
		WithPerIPRatePerSecond(100),
		WithPerIPMaxInFlight(100),
		WithServerBurst(serverCapacity),
		WithServerRatePerSecond(0), // no refill
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Requests from different IPs should all draw from the server bucket
	var successCount int
	for i := 0; i < serverCapacity+5; i++ {
		r := httptest.NewRequest("GET", "/test", nil)
		r.RemoteAddr = fmt.Sprintf("10.0.0.%d:8080", i)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode == http.StatusOK {
			successCount++
		}
	}

	if successCount != serverCapacity {
		t.Errorf("Expected exactly %d successful requests, got %d", serverCapacity, successCount)
	}
}

func TestLoadShedder_ServerInFlightLimiting(t *testing.T) {
	serverMax := int32(3)
	ls := New(
		WithPerIPBurst(100),
		WithPerIPRatePerSecond(100),
		WithPerIPMaxInFlight(100),
		WithServerMaxInFlight(serverMax),
	)
	defer ls.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	results := make([]int, int(serverMax)+2)

	// Launch more requests than the server limit from different IPs
	for i := 0; i < int(serverMax)+2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r := httptest.NewRequest("GET", "/test", nil)
			r.RemoteAddr = fmt.Sprintf("10.0.0.%d:8080", idx)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			results[idx] = w.Result().StatusCode
		}(i)
	}

	// Wait for serverMax requests to enter the handler
	for i := 0; i < int(serverMax); i++ {
		<-started
	}

	// Give rejected requests time to complete
	time.Sleep(50 * time.Millisecond)

	// Release all blocked handlers
	close(release)
	// Drain any remaining started signals
	go func() {
		for range started {
		}
	}()
	wg.Wait()
	close(started)

	var okCount, rejectedCount int
	for _, code := range results {
		switch code {
		case http.StatusOK:
			okCount++
		case http.StatusTooManyRequests:
			rejectedCount++
		}
	}

	if okCount != int(serverMax) {
		t.Errorf("Expected %d OK responses, got %d", serverMax, okCount)
	}
	if rejectedCount != 2 {
		t.Errorf("Expected 2 rejected responses, got %d", rejectedCount)
	}
}

func TestLoadShedder_ServerAndPerIPComposition(t *testing.T) {
	// Server: 6 tokens, per-IP: 2 tokens, no refill.
	// Server tokens are consumed before per-IP checks, so a per-IP
	// rejection still costs a server token.
	ls := New(
		WithPerIPBurst(2),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(100),
		WithServerBurst(6),
		WithServerRatePerSecond(0),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// IP A: 2 succeed (per-IP allows 2), 3rd rejected by per-IP.
	// Server tokens consumed: 3 (one wasted on per-IP rejection).
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("IP A request %d should succeed", i)
		}
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Fatal("IP A 3rd request should be rejected by per-IP rate limit")
	}

	// IP B: 2 succeed. Server tokens consumed: 5 total.
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.2:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("IP B request %d should succeed", i)
		}
	}

	// IP C: 1 server token left → 1st succeeds, 2nd rejected by server rate limit.
	r = httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.3:8080"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatal("IP C 1st request should succeed (1 server token left)")
	}

	r = httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.3:8080"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Fatal("IP C 2nd request should be rejected by server rate limit")
	}
}

func TestLoadShedder_ServerDisabledByDefault(t *testing.T) {
	ls := New(
		WithPerIPBurst(5),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(100),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Without server options, different IPs each get their own per-IP bucket
	for i := 0; i < 20; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = fmt.Sprintf("10.0.0.%d:8080", i)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("Request %d from unique IP should succeed without server limits", i)
		}
	}
}

func TestLoadShedder_ServerRateLimit_ConcurrentAccess(t *testing.T) {
	capacity := 50
	ls := New(
		WithPerIPBurst(1000),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1000),
		WithServerBurst(capacity),
		WithServerRatePerSecond(0),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	numGoroutines := 200
	var wg sync.WaitGroup
	var successCount int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = fmt.Sprintf("10.%d.%d.%d:8080", id/65536, (id/256)%256, id%256)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Result().StatusCode == http.StatusOK {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	if successCount != int64(capacity) {
		t.Errorf("Expected exactly %d successful requests, got %d", capacity, successCount)
	}
}

func TestLoadShedder_ServerInflight_Decrement(t *testing.T) {
	ls := New(
		WithPerIPBurst(0),           // per-IP rejects everything
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(100),
		WithServerMaxInFlight(100),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Send requests that pass server in-flight but get rejected by per-IP rate limit
	for i := 0; i < 10; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = fmt.Sprintf("10.0.0.%d:8080", i)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusTooManyRequests {
			t.Fatal("Should be rejected by per-IP rate limit")
		}
	}

	// Server in-flight should be back to zero
	if v := atomic.LoadInt32(&ls.serverInflight); v != 0 {
		t.Errorf("Server in-flight should be 0 after all requests complete, got %d", v)
	}
}

func TestLoadShedder_CleanupDuringInFlight(t *testing.T) {
	maxInFlight := int32(3)
	ls := New(
		WithPerIPBurst(100),
		WithPerIPRatePerSecond(100),
		WithPerIPMaxInFlight(maxInFlight),
	)
	defer ls.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	ip := "10.0.0.1:8080"

	// Fill up all in-flight slots
	var wg sync.WaitGroup
	for i := 0; i < int(maxInFlight); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = ip
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
		}()
		<-started
	}

	// Run aggressive cleanup while requests are in-flight.
	// In-flight counters are > 0, so they survive cleanup.
	// Rate limit buckets are evicted (future cutoff).
	futureCutoff := time.Now().Add(time.Hour).Unix()
	ls.Cleanup(context.Background(), futureCutoff)

	// Next request from the same IP must be rejected — all
	// in-flight slots are occupied.
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = ip
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Errorf("Expected 429 while all slots occupied after cleanup, got %d", w.Result().StatusCode)
	}

	// Release blocked handlers and verify they all succeeded.
	close(release)
	go func() {
		for range started {
		}
	}()
	wg.Wait()
	close(started)
}

func TestLoadShedder_OptionPanicsOnNegative(t *testing.T) {
	tests := []struct {
		name string
		opt  func() Option
	}{
		{"WithPerIPBurst", func() Option { return WithPerIPBurst(-1) }},
		{"WithPerIPRatePerSecond", func() Option { return WithPerIPRatePerSecond(-1) }},
		{"WithPerIPMaxInFlight", func() Option { return WithPerIPMaxInFlight(-1) }},
		{"WithServerBurst", func() Option { return WithServerBurst(-1) }},
		{"WithServerRatePerSecond", func() Option { return WithServerRatePerSecond(-1) }},
		{"WithServerMaxInFlight", func() Option { return WithServerMaxInFlight(-1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("%s(-1) should panic", tt.name)
				}
			}()
			ls := New(tt.opt())
			ls.Stop()
		})
	}
}

func TestLoadShedder_ShedCauseCounters(t *testing.T) {
	ls := New(
		WithPerIPBurst(1),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(100),
		WithServerBurst(3),
		WithServerRatePerSecond(0),
		WithServerMaxInFlight(100),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// IP A: 1st succeeds (per-IP burst=1), 2nd rejected by per-IP rate.
	// Server tokens consumed: 2 (one on the per-IP rejection path).
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}

	if got := atomic.LoadInt64(&ls.sheddedCausePerIPRate); got != 1 {
		t.Errorf("sheddedCausePerIPRate = %d, want 1", got)
	}

	// IP B: 1st succeeds (1 server token left = 3-2=1), 2nd rejected by server rate.
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.2:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}

	if got := atomic.LoadInt64(&ls.sheddedCauseServerRate); got != 1 {
		t.Errorf("sheddedCauseServerRate = %d, want 1", got)
	}

	if got := atomic.LoadInt64(&ls.sheddedRequestCount); got != 2 {
		t.Errorf("sheddedRequestCount = %d, want 2", got)
	}
}

func TestLoadShedder_ShedCauseInFlightCounters(t *testing.T) {
	ls := New(
		WithPerIPBurst(100),
		WithPerIPRatePerSecond(100),
		WithPerIPMaxInFlight(1),
		WithServerMaxInFlight(2),
	)
	defer ls.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup

	// IP A: occupy 1 per-IP slot (also occupies 1 server slot)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}()
	<-started

	// IP A again: rejected by per-IP in-flight (limit=1)
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Fatal("Expected per-IP in-flight rejection")
	}

	// IP B: occupy 1 per-IP slot (now 2 server slots occupied)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.2:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}()
	<-started

	// IP C: rejected by server in-flight (limit=2, both slots full)
	r = httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.3:8080"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Fatal("Expected server in-flight rejection")
	}

	close(release)
	go func() {
		for range started {
		}
	}()
	wg.Wait()
	close(started)

	if got := atomic.LoadInt64(&ls.sheddedCausePerIPInFlight); got != 1 {
		t.Errorf("sheddedCausePerIPInFlight = %d, want 1", got)
	}
	if got := atomic.LoadInt64(&ls.sheddedCauseServerInFlight); got != 1 {
		t.Errorf("sheddedCauseServerInFlight = %d, want 1", got)
	}
}

func TestLoadShedder_ShedCountInvariant(t *testing.T) {
	// sheddedRequestCount must equal the sum of all four cause counters.
	ls := New(
		WithPerIPBurst(1),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1),
		WithServerBurst(4),
		WithServerRatePerSecond(0),
		WithServerMaxInFlight(3),
	)
	defer ls.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup

	// IP A: occupy 1 per-IP slot + 1 server slot
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}()
	<-started

	// IP A again: rejected by per-IP in-flight (slot full)
	func() {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}()

	// IP B: occupy 1 per-IP slot + 1 server slot (2 server total)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.2:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}()
	<-started

	// IP C: occupy 1 per-IP slot + 1 server slot (3 server total = max)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.3:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}()
	<-started

	// IP D: rejected by server in-flight (3 slots full)
	func() {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.4:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}()

	// Release all, drain started
	close(release)
	go func() {
		for range started {
		}
	}()
	wg.Wait()
	close(started)

	// Now trigger per-IP rate rejection: IP E gets 1 token (burst=1), 2nd rejected
	func() {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.5:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r) // succeeds
	}()
	func() {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.5:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r) // per-IP rate reject
	}()

	// Server rate: all 4 server tokens consumed (A, B, C, E succeeded).
	// IP F: rejected by server rate limit.
	func() {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.6:8080"
		handler.ServeHTTP(httptest.NewRecorder(), r) // server rate reject
	}()

	total := atomic.LoadInt64(&ls.sheddedRequestCount)
	sum := atomic.LoadInt64(&ls.sheddedCausePerIPInFlight) +
		atomic.LoadInt64(&ls.sheddedCausePerIPRate) +
		atomic.LoadInt64(&ls.sheddedCauseServerInFlight) +
		atomic.LoadInt64(&ls.sheddedCauseServerRate)

	if total != sum {
		t.Errorf("sheddedRequestCount (%d) != sum of cause counters (%d): "+
			"perIPInFlight=%d perIPRate=%d serverInFlight=%d serverRate=%d",
			total, sum,
			atomic.LoadInt64(&ls.sheddedCausePerIPInFlight),
			atomic.LoadInt64(&ls.sheddedCausePerIPRate),
			atomic.LoadInt64(&ls.sheddedCauseServerInFlight),
			atomic.LoadInt64(&ls.sheddedCauseServerRate))
	}
	if total == 0 {
		t.Error("Expected at least some rejections")
	}
}
