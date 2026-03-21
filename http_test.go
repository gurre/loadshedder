package loadshedder

import (
	"context"
	"fmt"
	"math"
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
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: burst, Rate: ratePerSecond, MaxInFlight: 10},
	})
	defer ls.Stop()

	fastHandler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

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

func TestConfig_Validate_NilBothPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("New(Config{}) should panic")
		}
	}()
	ls := New(Config{})
	ls.Stop()
}

func TestConfig_Validate_EmptyPerIPPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("New(Config{PerIP: &PerIPConfig{}}) should panic")
		}
	}()
	ls := New(Config{PerIP: &PerIPConfig{}})
	ls.Stop()
}

func TestConfig_Validate_EmptyServerPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("New(Config{Server: &ServerConfig{}}) should panic")
		}
	}()
	ls := New(Config{Server: &ServerConfig{}})
	ls.Stop()
}

func TestConfig_Validate_NegativeValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"PerIP.Burst", Config{PerIP: &PerIPConfig{Burst: -1}}},
		{"PerIP.Rate", Config{PerIP: &PerIPConfig{Rate: -1}}},
		{"PerIP.MaxInFlight", Config{PerIP: &PerIPConfig{MaxInFlight: -1}}},
		{"Server.Burst", Config{Server: &ServerConfig{Burst: -1}}},
		{"Server.Rate", Config{Server: &ServerConfig{Rate: -1}}},
		{"Server.MaxInFlight", Config{Server: &ServerConfig{MaxInFlight: -1}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err == nil {
				t.Errorf("Expected error for %s with negative value", tt.name)
			}
		})
	}
}

func TestConfig_Validate_MaxInFlightOverflow(t *testing.T) {
	t.Run("PerIP", func(t *testing.T) {
		cfg := Config{PerIP: &PerIPConfig{MaxInFlight: math.MaxInt32 + 1}}
		if err := cfg.Validate(); err == nil {
			t.Error("Expected error for PerIP.MaxInFlight > MaxInt32")
		}
	})
	t.Run("Server", func(t *testing.T) {
		cfg := Config{Server: &ServerConfig{MaxInFlight: math.MaxInt32 + 1}}
		if err := cfg.Validate(); err == nil {
			t.Error("Expected error for Server.MaxInFlight > MaxInt32")
		}
	})
}

func TestLoadShedder_AllPathsRateLimited(t *testing.T) {
	// After the single server token is consumed, every path is rejected.
	ls := New(Config{
		Server: &ServerConfig{Burst: 1, Rate: 0},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Consume the single token
	r := httptest.NewRequest("GET", "/setup", nil)
	r.RemoteAddr = "192.168.1.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("First request should succeed, got %d", w.Result().StatusCode)
	}

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

func TestLoadShedder_RateOnlyWithoutMaxInFlight(t *testing.T) {
	// Setting only burst/rate without MaxInFlight must not reject requests.
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 10, Rate: 10},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 5; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("Request %d should succeed with rate-only config, got %d", i, w.Result().StatusCode)
		}
	}
}

func TestLoadShedder_PerIPConcurrencyOnly(t *testing.T) {
	// Concurrency-only per-IP config (no rate limit) must allow requests
	// and not create zero-token buckets.
	ls := New(Config{
		PerIP: &PerIPConfig{MaxInFlight: 5},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 10; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("Request %d should succeed with concurrency-only config, got %d", i, w.Result().StatusCode)
		}
	}
}

func TestLoadShedder_InFlightLimiting(t *testing.T) {
	maxInFlight := 2
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 100, Rate: 100, MaxInFlight: maxInFlight},
	})
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
	for i := 0; i < maxInFlight; i++ {
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

	if okCount != maxInFlight {
		t.Errorf("Expected %d OK responses, got %d", maxInFlight, okCount)
	}
	if rejectedCount != totalRequests-maxInFlight {
		t.Errorf("Expected %d rejected responses, got %d", totalRequests-maxInFlight, rejectedCount)
	}
}

func TestLoadShedder_RequestCount(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 3, Rate: 0, MaxInFlight: 100},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Burst defaults rate to 3. 3 accepted + 2 rejected = 5 total observed
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
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10},
	})
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
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 2, Rate: 0, MaxInFlight: 10},
	})
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
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 50, Rate: 0, MaxInFlight: 100},
	})
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
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 10, Rate: 1, MaxInFlight: 10},
	})
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
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 10, Rate: 1, MaxInFlight: 10},
	})

	// Stop must not panic when called multiple times.
	ls.Stop()
	ls.Stop()
	ls.Stop()
}

func TestLoadShedder_RefillBehavior(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 2, Rate: 2, MaxInFlight: 10},
	})
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
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 100, Rate: 100, MaxInFlight: 100},
		Server: &ServerConfig{Burst: serverCapacity, Rate: 0},
	})
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
	serverMax := 3
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 100, Rate: 100, MaxInFlight: 100},
		Server: &ServerConfig{MaxInFlight: serverMax},
	})
	defer ls.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	results := make([]int, serverMax+2)

	// Launch more requests than the server limit from different IPs
	for i := 0; i < serverMax+2; i++ {
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
	for i := 0; i < serverMax; i++ {
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

	if okCount != serverMax {
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
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 2, Rate: 0, MaxInFlight: 100},
		Server: &ServerConfig{Burst: 6, Rate: 0},
	})
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
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 5, Rate: 0, MaxInFlight: 100},
	})
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
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 1000, Rate: 0, MaxInFlight: 1000},
		Server: &ServerConfig{Burst: capacity, Rate: 0},
	})
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
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 100},
		Server: &ServerConfig{MaxInFlight: 100},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Send requests — per-IP burst=1, so second from same IP is rejected by rate limit
	for i := 0; i < 10; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}

	// Server in-flight should be back to zero
	if v := atomic.LoadInt32(&ls.serverInflight); v != 0 {
		t.Errorf("Server in-flight should be 0 after all requests complete, got %d", v)
	}
}

func TestLoadShedder_CleanupDuringInFlight(t *testing.T) {
	maxInFlight := 3
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 100, Rate: 100, MaxInFlight: maxInFlight},
	})
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
	for i := 0; i < maxInFlight; i++ {
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

func TestLoadShedder_ShedCauseCounters(t *testing.T) {
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 100},
		Server: &ServerConfig{Burst: 3, Rate: 0, MaxInFlight: 100},
	})
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
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 100, Rate: 100, MaxInFlight: 1},
		Server: &ServerConfig{MaxInFlight: 2},
	})
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
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 1},
		Server: &ServerConfig{Burst: 4, Rate: 0, MaxInFlight: 3},
	})
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

func TestLoadShedder_ServerOnly_ServerLimitsEnforced(t *testing.T) {
	ls := New(Config{
		Server: &ServerConfig{Burst: 3, Rate: 0},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var ok, rejected int
	for i := 0; i < 5; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = fmt.Sprintf("10.0.0.%d:8080", i)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode == http.StatusOK {
			ok++
		} else {
			rejected++
		}
	}

	if ok != 3 {
		t.Errorf("Expected 3 OK, got %d", ok)
	}
	if rejected != 2 {
		t.Errorf("Expected 2 rejected, got %d", rejected)
	}
}

func TestLoadShedder_WithTrustedProxyHeader_IndependentBuckets(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Two requests with different header IPs from the same RemoteAddr
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	r1.Header.Set("X-Forwarded-For", "203.0.113.1")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.1:8080"
	r2.Header.Set("X-Forwarded-For", "203.0.113.2")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK || w2.Result().StatusCode != http.StatusOK {
		t.Error("Different header IPs should get independent buckets")
	}
}

func TestLoadShedder_WithTrustedProxyHeader_SharedBucketBehindProxies(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Same real IP behind different ALBs (different RemoteAddr)
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	r1.Header.Set("X-Forwarded-For", "203.0.113.99")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.2:8080"
	r2.Header.Set("X-Forwarded-For", "203.0.113.99")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request should succeed")
	}
	if w2.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Second request from same real IP should be rejected (burst=1)")
	}
}

func TestLoadShedder_WithTrustedProxyHeader_MissingHeaderFallback(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// No header set — falls back to RemoteAddr
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.1:8080"
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request should succeed")
	}
	if w2.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Second request should be rejected (same RemoteAddr, burst=1)")
	}
}

func TestLoadShedder_WithTrustedProxyHeader_EmptyHeaderFallback(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Header present but empty — falls back to RemoteAddr
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	r1.Header.Set("X-Forwarded-For", "")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.1:8080"
	r2.Header.Set("X-Forwarded-For", "")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request should succeed")
	}
	if w2.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Second request should be rejected (same RemoteAddr, burst=1)")
	}
}

func TestLoadShedder_WithTrustedProxyHeader_MultipleIPsUsesLast(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Same last IP but different proxy chains — should share bucket
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	r1.Header.Set("X-Forwarded-For", "203.0.113.1, 10.1.1.1, 10.2.2.2")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.2:8080"
	r2.Header.Set("X-Forwarded-For", "203.0.113.99, 10.2.2.2")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request should succeed")
	}
	if w2.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Second request with same last IP should be rejected")
	}
}

func TestLoadShedder_WithTrustedProxyHeader_DegenerateHeaders(t *testing.T) {
	// Degenerate headers should fall back to RemoteAddr and not panic.
	// We verify the fallback by sending two requests: one with a degenerate
	// header and one without, both from the same RemoteAddr. If they share
	// a bucket (second is rejected), the fallback is working.
	degenerateValues := []string{
		",",          // comma-only → rightmost is empty
		"  ",         // spaces only → trimmed to empty
		", 1.2.3.4",  // leading comma → rightmost is "1.2.3.4" (valid, not degenerate)
		"not-an-ip",  // garbage → ParseAddr fails
		"1.2.3.4,",   // trailing comma → rightmost is empty
		", ",          // comma-space → rightmost trimmed to empty
	}

	for _, val := range degenerateValues {
		t.Run(val, func(t *testing.T) {
			// Fresh LoadShedder per subtest so buckets are independent.
			sub := New(Config{
				PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
			})
			defer sub.Stop()
			h := sub.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			// First request with degenerate header
			r1 := httptest.NewRequest("GET", "/", nil)
			r1.RemoteAddr = "10.0.0.1:8080"
			r1.Header.Set("X-Forwarded-For", val)
			w1 := httptest.NewRecorder()
			h.ServeHTTP(w1, r1)
			if w1.Result().StatusCode != http.StatusOK {
				t.Fatalf("First request should succeed, got %d", w1.Result().StatusCode)
			}

			// Second request with no header (pure RemoteAddr) from same IP.
			// If the degenerate header fell back to RemoteAddr, this request
			// shares the same bucket and should be rejected (burst=1).
			r2 := httptest.NewRequest("GET", "/", nil)
			r2.RemoteAddr = "10.0.0.1:8080"
			w2 := httptest.NewRecorder()
			h.ServeHTTP(w2, r2)

			// ", 1.2.3.4" has a valid rightmost IP, so it won't share
			// a bucket with RemoteAddr 10.0.0.1 — second request passes.
			if val == ", 1.2.3.4" {
				if w2.Result().StatusCode != http.StatusOK {
					t.Errorf("Valid rightmost IP should not fall back to RemoteAddr")
				}
				return
			}
			if w2.Result().StatusCode != http.StatusTooManyRequests {
				t.Errorf("Degenerate header %q should fall back to RemoteAddr, but second request was not rejected", val)
			}
		})
	}
}

func TestLoadShedder_WithTrustedProxyHeader_BracketedIPv6(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// "[::1]" in a header should be parsed as "::1" (brackets stripped).
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	r1.Header.Set("X-Forwarded-For", "[::1]")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	// Bare "::1" should share the same bucket.
	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.2:8080"
	r2.Header.Set("X-Forwarded-For", "::1")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request should succeed")
	}
	if w2.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("[::1] and ::1 should share bucket")
	}
}

func TestLoadShedder_WithTrustedProxyHeader_MultipleHeaders(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Two XFF headers: rightmost value from the last header entry is used.
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	r1.Header["X-Forwarded-For"] = []string{"1.1.1.1", "2.2.2.2, 3.3.3.3"}
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	// Second request with the same rightmost IP from last header
	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.2:8080"
	r2.Header["X-Forwarded-For"] = []string{"9.9.9.9, 3.3.3.3"}
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request should succeed")
	}
	if w2.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Second request with same rightmost IP should be rejected (burst=1)")
	}
}

func TestLoadShedder_WithTrustedProxyHeader_IPv6Normalized(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1, Rate: 0, MaxInFlight: 10, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Different textual representations of the same IPv6 address
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "10.0.0.1:8080"
	r1.Header.Set("X-Forwarded-For", "2001:0db8:0000:0000:0000:0000:0000:0001")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "10.0.0.2:8080"
	r2.Header.Set("X-Forwarded-For", "2001:db8::1")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, r2)

	if w1.Result().StatusCode != http.StatusOK {
		t.Error("First request should succeed")
	}
	if w2.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Same IPv6 address in different forms should share bucket")
	}
}

func TestLoadShedder_RateWithoutBurst_DefaultsBurstToRate(t *testing.T) {
	// Setting only rate should default burst to rate, not silently reject all.
	ls := New(Config{
		PerIP: &PerIPConfig{Rate: 10, MaxInFlight: 100},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// With burst defaulted to 10, the first 10 requests should succeed.
	var ok int
	for i := 0; i < 15; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode == http.StatusOK {
			ok++
		}
	}
	if ok != 10 {
		t.Errorf("Expected 10 OK (burst defaulted to rate), got %d", ok)
	}
}

func TestLoadShedder_BurstWithoutRate_DefaultsRateToBurst(t *testing.T) {
	// Setting only burst should default rate to burst, enabling refill.
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 2, MaxInFlight: 100},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Exhaust the burst
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("Request %d should succeed", i)
		}
	}

	// Should be rejected now
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Should be rejected after burst exhausted")
	}

	// After refill (rate defaulted to burst=2), tokens should be available
	time.Sleep(1100 * time.Millisecond)
	r = httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusOK {
		t.Error("Should succeed after refill (rate defaulted to burst)")
	}
}

func TestLoadShedder_ServerRateWithoutBurst_DefaultsBurst(t *testing.T) {
	// Server rate without burst should also default burst.
	ls := New(Config{
		Server: &ServerConfig{Rate: 5},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// With burst defaulted to 5, first 5 requests should succeed.
	var ok int
	for i := 0; i < 8; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = fmt.Sprintf("10.0.0.%d:8080", i)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode == http.StatusOK {
			ok++
		}
	}
	if ok != 5 {
		t.Errorf("Expected 5 OK (server burst defaulted to rate), got %d", ok)
	}
}

func TestLoadShedder_HashStability(t *testing.T) {
	// Within a single process, the same input must produce the same hash.
	h1 := murmur3Sum32("192.168.1.1")
	h2 := murmur3Sum32("192.168.1.1")
	if h1 != h2 {
		t.Errorf("Hash not deterministic within process: %d != %d", h1, h2)
	}

	// Different inputs should (almost certainly) produce different hashes.
	h3 := murmur3Sum32("192.168.1.2")
	if h1 == h3 {
		t.Error("Different inputs produced the same hash (extremely unlikely)")
	}
}

func TestConfig_Validate_BothNonNilBothEmpty(t *testing.T) {
	// Both PerIP and Server non-nil but with all-zero fields — both should
	// fail validation. Validate returns the first error (PerIP checked first).
	cfg := Config{
		PerIP:  &PerIPConfig{},
		Server: &ServerConfig{},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("Expected error when both configs are non-nil with all-zero fields")
	}
}

func TestConfig_Validate_ValidConfigs(t *testing.T) {
	// Verify that well-formed configs pass validation.
	valid := []Config{
		{PerIP: &PerIPConfig{Burst: 10}},
		{PerIP: &PerIPConfig{Rate: 10}},
		{PerIP: &PerIPConfig{MaxInFlight: 10}},
		{Server: &ServerConfig{Burst: 10}},
		{Server: &ServerConfig{Rate: 10}},
		{Server: &ServerConfig{MaxInFlight: 10}},
		{PerIP: &PerIPConfig{Burst: 10}, Server: &ServerConfig{MaxInFlight: 5}},
	}
	for i, cfg := range valid {
		if err := cfg.Validate(); err != nil {
			t.Errorf("Config %d should be valid, got: %v", i, err)
		}
	}
}

func TestLoadShedder_ServerOnly_NoCleanupGoroutine(t *testing.T) {
	// Server-only config should not start the background cleanup goroutine.
	// Verify by checking that Stop doesn't block (channel is never written to
	// by a cleanup goroutine, so close is safe and immediate).
	ls := New(Config{
		Server: &ServerConfig{Burst: 100, Rate: 10},
	})

	// perIPRateEnabled and perIPInFlightEnabled should both be false
	if ls.perIPRateEnabled {
		t.Error("perIPRateEnabled should be false for server-only config")
	}
	if ls.perIPInFlightEnabled {
		t.Error("perIPInFlightEnabled should be false for server-only config")
	}

	ls.Stop()
}

func TestLoadShedder_OrthogonalConfig_PerIPRateServerInFlight(t *testing.T) {
	// Per-IP rate-only + server concurrency-only: the two dimensions
	// should compose independently.
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 2, Rate: 0},
		Server: &ServerConfig{MaxInFlight: 100},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First 2 requests succeed (per-IP burst=2), 3rd rejected by per-IP rate
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("Request %d should succeed, got %d", i, w.Result().StatusCode)
		}
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("3rd request should be rejected by per-IP rate limit")
	}
}

func TestLoadShedder_OrthogonalConfig_PerIPInFlightServerRate(t *testing.T) {
	// Per-IP concurrency-only + server rate-only: the two dimensions
	// should compose independently.
	started := make(chan struct{})
	release := make(chan struct{})

	ls := New(Config{
		PerIP:  &PerIPConfig{MaxInFlight: 1},
		Server: &ServerConfig{Burst: 100, Rate: 0},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	// Occupy the per-IP in-flight slot
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}()
	<-started

	// Same IP: rejected by per-IP in-flight
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Error("Same IP should be rejected by per-IP in-flight limit")
	}

	close(release)
	go func() { for range started {} }()
	wg.Wait()
	close(started)
}

func TestLoadShedder_ConcurrentWithProxy(t *testing.T) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1000, Rate: 0, MaxInFlight: 100, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = "10.0.0.1:8080"
			r.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", id%256))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
		}(i)
	}
	wg.Wait()
}
