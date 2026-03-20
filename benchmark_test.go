package loadshedder

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// BenchmarkLoadShedder_Handler_Rejection measures the cost of rejecting a
// request via per-IP in-flight limit (the cheapest rejection path after
// the server-wide checks).
func BenchmarkLoadShedder_Handler_Rejection(b *testing.B) {
	ls := New(
		WithPerIPBurst(1<<30),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(0), // reject everything at in-flight check
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
	ls := New(
		WithPerIPBurst(100),
		WithPerIPRatePerSecond(10),
		WithPerIPMaxInFlight(10),
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
		WithPerIPBurst(1000),
		WithPerIPRatePerSecond(100),
		WithPerIPMaxInFlight(50),
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
		WithPerIPBurst(1<<30),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1<<30),
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
		WithPerIPBurst(1<<30),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1<<30),
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
	ls := New(
		WithPerIPBurst(1<<30),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1<<30),
	)
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
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "req/s")
	}
}

// BenchmarkLoadShedder_MaxThroughput_SingleIP measures the worst-case
// throughput where all cores contend on a single client bucket.
func BenchmarkLoadShedder_MaxThroughput_SingleIP(b *testing.B) {
	ls := New(
		WithPerIPBurst(1<<30),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1<<30),
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

// BenchmarkLoadShedder_ServerRateLimit benchmarks overhead of server-wide token bucket
func BenchmarkLoadShedder_ServerRateLimit(b *testing.B) {
	ls := New(
		WithPerIPBurst(1<<30),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1<<30),
		WithServerBurst(1<<30),
		WithServerRatePerSecond(0),
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
}

// BenchmarkLoadShedder_ServerAndPerIP benchmarks combined server-wide + per-IP overhead
func BenchmarkLoadShedder_ServerAndPerIP(b *testing.B) {
	ls := New(
		WithPerIPBurst(1<<30),
		WithPerIPRatePerSecond(0),
		WithPerIPMaxInFlight(1<<30),
		WithServerBurst(1<<30),
		WithServerRatePerSecond(0),
		WithServerMaxInFlight(1<<30),
	)
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

// BenchmarkLoadShedder_HighContention benchmarks high contention scenario
// where all goroutines share a single IP with a small bucket and low
// in-flight limit, maximizing cross-core contention on the same shard.
func BenchmarkLoadShedder_HighContention(b *testing.B) {
	ls := New(
		WithPerIPBurst(10),
		WithPerIPRatePerSecond(5),
		WithPerIPMaxInFlight(5),
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
