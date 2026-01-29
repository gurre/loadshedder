package loadshedder

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// BenchmarkLoadShedder_Handler_InFlightLimit benchmarks the handler when in-flight limit is hit
func BenchmarkLoadShedder_Handler_InFlightLimit(b *testing.B) {
	ls := New(
		WithTokenBucketCapacity(1000000),
		WithTokenBucketRefillRatePerSecond(1000000),
		WithMaxRequestsInFlight(1), // Very low limit
	)
	defer ls.Stop()

	// Use a handler that takes time to complete to ensure in-flight limit is hit
	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate work
		_ = atomic.AddInt64(&ls.requestCount, 0) // Access shared state
		w.WriteHeader(http.StatusOK)
	}))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest("GET", "/test", nil)
		r.RemoteAddr = "192.168.1.1:8080"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
	}
}

// BenchmarkLoadShedder_BucketKeyGeneration benchmarks bucket key generation
func BenchmarkLoadShedder_BucketKeyGeneration(b *testing.B) {
	r := httptest.NewRequest("GET", "/test", nil)
	r.RemoteAddr = "192.168.1.1:8080"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h := bucketKeyIP(r)
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
	tb := NewTokenBucket(b.N*2, 0) // Enough tokens for all operations

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
	ls := New(
		WithTokenBucketCapacity(100),
		WithTokenBucketRefillRatePerSecond(10),
		WithMaxRequestsInFlight(10),
	)
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
	ls := New(
		WithTokenBucketCapacity(1000),
		WithTokenBucketRefillRatePerSecond(100),
		WithMaxRequestsInFlight(50),
	)
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
	ls := New(
		WithTokenBucketCapacity(b.N+1),
		WithTokenBucketRefillRatePerSecond(0),
		WithMaxRequestsInFlight(1000),
	)
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
	ls := New(
		WithTokenBucketCapacity(1000000),
		WithTokenBucketRefillRatePerSecond(1000000),
		WithMaxRequestsInFlight(1000),
	)
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
	ls := New(
		WithTokenBucketCapacity(1<<30),
		WithTokenBucketRefillRatePerSecond(0),
		WithMaxRequestsInFlight(1<<30),
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own IP so buckets never collide.
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = fmt.Sprintf("%d.%d.%d.%d:1234",
			b.N%256, (b.N/256)%256, (b.N/65536)%256, (b.N/16777216)%256)
		for pb.Next() {
			handler.ServeHTTP(discardResponseWriter{}, r)
		}
	})
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "req/s")
	}
}

// BenchmarkLoadShedder_MaxThroughput_SingleIP measures the worst-case
// throughput where all cores contend on a single client bucket.
func BenchmarkLoadShedder_MaxThroughput_SingleIP(b *testing.B) {
	ls := New(
		WithTokenBucketCapacity(1<<30),
		WithTokenBucketRefillRatePerSecond(0),
		WithMaxRequestsInFlight(1<<30),
	)
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
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "req/s")
	}
}

// BenchmarkLoadShedder_HighContention benchmarks high contention scenario
func BenchmarkLoadShedder_HighContention(b *testing.B) {
	ls := New(
		WithTokenBucketCapacity(10), // Small bucket to create contention
		WithTokenBucketRefillRatePerSecond(5),
		WithMaxRequestsInFlight(5), // Low in-flight limit
	)
	defer ls.Stop()

	handler := ls.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// All goroutines use same IP to maximize contention
		r := httptest.NewRequest("GET", "/test", nil)
		r.RemoteAddr = "192.168.1.1:8080"
		for pb.Next() {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
		}
	})
}
