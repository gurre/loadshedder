package loadshedder

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// BenchmarkLoadShedder_Handler_Rejection measures the cost of rejecting a
// request via per-IP in-flight limit. A background goroutine holds the
// single in-flight slot, so every benchmark iteration takes the
// increment-then-revert rejection path.
func BenchmarkLoadShedder_Handler_Rejection(b *testing.B) {
	started := make(chan struct{})
	release := make(chan struct{})

	ls := New(Config{
		PerIP: &PerIPConfig{MaxInFlight: 1},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
	}))

	// Occupy the single in-flight slot
	go func() {
		r := httptest.NewRequest("GET", "/test", nil)
		r.RemoteAddr = "192.168.1.1:8080"
		handler.ServeHTTP(discardResponseWriter{}, r)
	}()
	<-started

	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(discardResponseWriter{}, r)
	}
	close(release)
}

// BenchmarkLoadShedder_BucketKeyGeneration benchmarks bucket key generation
func BenchmarkLoadShedder_BucketKeyGeneration(b *testing.B) {
	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := bucketKeyIP(r, "")
		_ = h
		_ = h ^ goldenRatio
	}
}

// BenchmarkLoadShedder_TokenBucketCreation benchmarks token bucket creation overhead
func BenchmarkLoadShedder_TokenBucketCreation(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewTokenBucket(100, 10)
	}
}

// BenchmarkLoadShedder_TokenBucket_TryTake_Success benchmarks successful token take
func BenchmarkLoadShedder_TokenBucket_TryTake_Success(b *testing.B) {
	tb := NewTokenBucket(1<<30, 0)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.TryTake()
	}
}

// BenchmarkLoadShedder_TokenBucket_TryTake_Failure benchmarks failed token take (empty bucket)
func BenchmarkLoadShedder_TokenBucket_TryTake_Failure(b *testing.B) {
	tb := NewTokenBucket(0, 0) // No tokens

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = tb.TryTake()
	}
}

// BenchmarkLoadShedder_TokenBucket_ConcurrentAccess benchmarks concurrent token bucket access
func BenchmarkLoadShedder_TokenBucket_ConcurrentAccess(b *testing.B) {
	tb := NewTokenBucket(1000000, 0) // Large bucket for benchmark

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = tb.TryTake()
		}
	})
}

// BenchmarkLoadShedder_TokenBucket_Refill benchmarks token bucket refill operations
func BenchmarkLoadShedder_TokenBucket_Refill(b *testing.B) {
	tb := NewTokenBucket(100, 1000) // Fast refill rate

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Exhaust tokens first
		for j := 0; j < 100; j++ {
			tb.TryTake()
		}
		// Then trigger refill
		_ = tb.TryTake()
	}
}

// BenchmarkLoadShedder_Cleanup benchmarks cleanup operation
func BenchmarkLoadShedder_Cleanup(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 100, Rate: 10, MaxInFlight: 10},
	})
	defer ls.Stop()

	// Populate buckets
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 1000; i++ {
		r := httptest.NewRequest("GET", "/test", nil)
		r.RemoteAddr = fmt.Sprintf("192.168.1.%d:8080", i%256)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cutoff := int64(i) // Vary cutoff to test different scenarios
		ls.Cleanup(context.TODO(), cutoff)
	}
}

// BenchmarkLoadShedder_ShardedMapOperations benchmarks sharded map operations
func BenchmarkLoadShedder_ShardedMapOperations(b *testing.B) {
	m := newShardedMap[*TokenBucket]()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := uint32(i % 1000)
			m.LoadOrStore(key, NewTokenBucket(100, 10))
			m.Load(key)
			if i%100 == 0 {
				m.Delete(key)
			}
			i++
		}
	})
}

// BenchmarkLoadShedder_FullRequestFlow benchmarks the complete request flow
func BenchmarkLoadShedder_FullRequestFlow(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1000, Rate: 100, MaxInFlight: 50},
	})
	defer ls.Stop()

	// Simulate realistic handler that does some work
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate some processing
		_ = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r := httptest.NewRequest("GET", "/api/v1/test", nil)
			r.RemoteAddr = fmt.Sprintf("192.168.%d.%d:8080", (i/256)%256, i%256)
			r.Header.Set("User-Agent", "benchmark-client/1.0")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			i++
		}
	})
}

// BenchmarkLoadShedder_HandlerOverhead isolates the handler's own cost by
// reusing a pre-built request and a minimal ResponseWriter, removing
// httptest allocation noise from the measurement.
func BenchmarkLoadShedder_HandlerOverhead(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(discardResponseWriter{}, r)
	}
}

// BenchmarkLoadShedder_HandlerOverhead_Parallel measures handler throughput
// across multiple goroutines sharing the same IP, isolating handler cost from
// httptest allocations.
func BenchmarkLoadShedder_HandlerOverhead_Parallel(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			handler.ServeHTTP(discardResponseWriter{}, r)
		}
	})
}

// discardResponseWriter is a no-op http.ResponseWriter that avoids httptest.NewRecorder allocations.
// Header() returns a fresh map because http.Error writes to it, which would
// race under parallel benchmarks with a shared map.
type discardResponseWriter struct{}

func (discardResponseWriter) Header() http.Header         { return http.Header{} }
func (discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (discardResponseWriter) WriteHeader(int)             {}

// BenchmarkLoadShedder_MaxThroughput measures the maximum requests per second
// the middleware can sustain across all cores. Each goroutine uses a distinct
// IP so buckets are distributed across shards (best-case, no cross-core
// contention on the same bucket). The noop handler and discardResponseWriter
// isolate middleware cost from application and httptest overhead.
func BenchmarkLoadShedder_MaxThroughput(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	var goroutineID atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own IP so buckets never collide.
		id := goroutineID.Add(1)
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = fmt.Sprintf("%d.%d.%d.%d:1234",
			id%256, (id/256)%256, (id/65536)%256, (id/16777216)%256)
		for pb.Next() {
			handler.ServeHTTP(discardResponseWriter{}, r)
		}
	})
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "reqs/s")
	}
}

// BenchmarkLoadShedder_MaxThroughput_SingleIP measures the worst-case
// throughput where all cores contend on a single client bucket.
func BenchmarkLoadShedder_MaxThroughput_SingleIP(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:1234"

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			handler.ServeHTTP(discardResponseWriter{}, r)
		}
	})
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "reqs/s")
	}
}

// BenchmarkLoadShedder_ServerRateLimit benchmarks overhead of server-wide token bucket
func BenchmarkLoadShedder_ServerRateLimit(b *testing.B) {
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
		Server: &ServerConfig{Burst: 1 << 30, Rate: 0},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:1234"

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			handler.ServeHTTP(discardResponseWriter{}, r)
		}
	})
}

// BenchmarkLoadShedder_ServerAndPerIP benchmarks combined server-wide + per-IP overhead
func BenchmarkLoadShedder_ServerAndPerIP(b *testing.B) {
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
		Server: &ServerConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	var goroutineID atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := goroutineID.Add(1)
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = fmt.Sprintf("%d.%d.%d.%d:1234",
			id%256, (id/256)%256, (id/65536)%256, (id/16777216)%256)
		for pb.Next() {
			handler.ServeHTTP(discardResponseWriter{}, r)
		}
	})
}

// BenchmarkLoadShedder_ServerOnly measures handler overhead without
// per-IP checks (server-only mode). Should be faster than HandlerOverhead.
func BenchmarkLoadShedder_ServerOnly(b *testing.B) {
	ls := New(Config{
		Server: &ServerConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(discardResponseWriter{}, r)
	}
}

// BenchmarkLoadShedder_WithTrustedProxyHeader measures handler overhead with
// header-based IP extraction vs RemoteAddr.
func BenchmarkLoadShedder_WithTrustedProxyHeader(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30, ProxyHeader: "X-Forwarded-For"},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "10.0.0.1:8080"
	r.Header.Set("X-Forwarded-For", "203.0.113.1, 10.1.1.1")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(discardResponseWriter{}, r)
	}
}

// benchHTTPClient returns an *http.Client with connection pooling configured
// for benchmark throughput (matches production ALB connection pools).
func benchHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			MaxConnsPerHost:     256,
		},
	}
}

// BenchmarkLoadShedder_APIBehindALB simulates a production API behind an
// AWS ALB: server-wide + per-IP limits, X-Forwarded-For header extraction,
// many distinct clients arriving through a small set of ALB IPs.
// Reports reqs/s as a custom metric.
func BenchmarkLoadShedder_APIBehindALB(b *testing.B) {
	ls := New(Config{
		PerIP:  &PerIPConfig{Burst: 100, Rate: 50, MaxInFlight: 20, ProxyHeader: "X-Forwarded-For"},
		Server: &ServerConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	ts := httptest.NewServer(handler)
	defer ts.Close()

	client := benchHTTPClient()
	url := ts.URL + "/api/v1/data"
	var goroutineID atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		id := goroutineID.Add(1)
		xff := fmt.Sprintf("203.0.%d.%d, 10.0.0.%d", (id/256)%256, id%256, id%4)
		for pb.Next() {
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				b.Fatal(err)
			}
			req.Header["X-Forwarded-For"] = []string{xff}
			resp, err := client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	})
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "reqs/s")
	}
}

// BenchmarkLoadShedder_APIBehindALB_ServerOnly simulates a production
// API where per-IP checks are disabled (server-only mode). Compares against
// APIBehindALB to show per-IP overhead on a real HTTP stack.
func BenchmarkLoadShedder_APIBehindALB_ServerOnly(b *testing.B) {
	ls := New(Config{
		Server: &ServerConfig{Burst: 1 << 30, Rate: 0, MaxInFlight: 1 << 30},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	ts := httptest.NewServer(handler)
	defer ts.Close()

	client := benchHTTPClient()
	url := ts.URL + "/api/v1/data"

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				b.Fatal(err)
			}
			resp, err := client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	})
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "reqs/s")
	}
}

// BenchmarkLoadShedder_HighContention benchmarks high contention scenario
// where all goroutines share a single IP with a small bucket and low
// in-flight limit, maximizing cross-core contention on the same shard.
func BenchmarkLoadShedder_HighContention(b *testing.B) {
	ls := New(Config{
		PerIP: &PerIPConfig{Burst: 10, Rate: 5, MaxInFlight: 5},
	})
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			handler.ServeHTTP(discardResponseWriter{}, r)
		}
	})
}
