# loadshedder

[![Go Reference](https://pkg.go.dev/badge/github.com/gurre/loadshedder.svg)](https://pkg.go.dev/github.com/gurre/loadshedder)

Ultra performant HTTP middleware that rejects excess traffic before it overwhelms your server.

It does four things:

1. **Server rate limiting** — a single token bucket shared by all clients caps the server-wide QPS.
2. **Server concurrency limiting** — a single atomic counter caps the total number of in-flight requests.
3. **Per-IP rate limiting** — each client gets a token bucket that refills over time. When the bucket is empty, requests are rejected with `429 Too Many Requests`.
4. **Per-IP concurrency limiting** — tracks how many requests each client has in flight. When the limit is reached, new requests are rejected with `429 Too Many Requests`.

All four dimensions are independently configurable and composable — a request must pass all active checks. At least one of `PerIP` or `Server` must be configured. Clients are identified by IP address.

## Install

```
go get github.com/gurre/loadshedder
```


```go
ls := loadshedder.New(loadshedder.Config{
    Server: &loadshedder.ServerConfig{
        Burst: 1000, Rate: 200, MaxInFlight: 100,
    },
    PerIP: &loadshedder.PerIPConfig{
        Burst: 200, Rate: 50, MaxInFlight: 20,
    },
})

mux := http.NewServeMux()
mux.HandleFunc("/", handleRoot)
mux.HandleFunc("/health", handleHealth)

server := &http.Server{
    Addr:    ":8080",
    Handler: ls.Handler(mux),
}

// Graceful shutdown
go func() {
    <-ctx.Done()
    server.Shutdown(context.Background())
    ls.Stop() // release the background cleanup goroutine
}()

server.ListenAndServe()
```

## Configuration

Configuration uses typed structs. `nil` means disabled.

| Field | What it controls |
|---|---|
| `ServerConfig.Burst` | Max tokens in the server-wide bucket (burst size) |
| `ServerConfig.Rate` | Sustained total request rate across all clients |
| `ServerConfig.MaxInFlight` | Max concurrent requests across all clients. Zero = disabled. |
| `PerIPConfig.Burst` | Max tokens per client bucket (burst size) |
| `PerIPConfig.Rate` | Sustained request rate per client |
| `PerIPConfig.MaxInFlight` | Max concurrent requests per client. Zero = disabled (rate limiting only). |
| `PerIPConfig.ProxyHeader` | Extract real client IP from a header (e.g. `X-Forwarded-For`), using the rightmost value. Empty = use RemoteAddr. |

When burst is set without rate (or vice versa), `New` defaults the missing value so the token bucket is usable. For example, `PerIPConfig{Rate: 10}` alone defaults burst to 10.

`New` panics if:
- Both `PerIP` and `Server` are nil (at least one must be configured)
- A non-nil config has all zero values (at least one limit required)
- Any value is negative
- `MaxInFlight` exceeds `math.MaxInt32`

### Deployment examples

#### Direct TCP (no proxy)

Clients connect directly. `RemoteAddr` is the real client IP.

```go
ls := loadshedder.New(loadshedder.Config{
    PerIP: &loadshedder.PerIPConfig{
        Burst: 200, Rate: 50, MaxInFlight: 20,
    },
})
defer ls.Stop()
http.ListenAndServe(":8080", ls.Handler(mux))
```

#### Behind a trusted reverse proxy (ALB, nginx, Envoy)

`RemoteAddr` is the proxy's IP — not the real client. Set `ProxyHeader` to extract the client IP from a trusted header. The rightmost comma-separated value is used because it is the IP appended by the nearest trusted proxy and cannot be forged by the client.

**Multi-hop warning:** This works for a single trusted proxy layer. In multi-hop setups (e.g. CDN → ALB → app), the rightmost value is the internal proxy IP, not the client. Use a dedicated header that contains only the client IP, or terminate `X-Forwarded-For` at the outermost proxy.

```go
ls := loadshedder.New(loadshedder.Config{
    PerIP: &loadshedder.PerIPConfig{
        Burst: 200, Rate: 50, MaxInFlight: 20,
        ProxyHeader: "X-Forwarded-For",
    },
    Server: &loadshedder.ServerConfig{
        Burst: 10000, Rate: 2000, MaxInFlight: 500,
    },
})
defer ls.Stop()
http.ListenAndServe(":8080", ls.Handler(mux))
```

#### Behind a proxy without a trusted header

When no trusted IP header is available and `RemoteAddr` is the proxy, use server-only limits.

```go
ls := loadshedder.New(loadshedder.Config{
    Server: &loadshedder.ServerConfig{
        Burst: 10000, Rate: 2000, MaxInFlight: 500,
    },
})
defer ls.Stop()
http.ListenAndServe(":8080", ls.Handler(mux))
```

#### Unix socket

On unix sockets, `RemoteAddr` is empty or a filesystem path — not a valid IP. Per-IP checks are meaningless because all connections would share a single bucket. Use server-only limits.

```go
ls := loadshedder.New(loadshedder.Config{
    Server: &loadshedder.ServerConfig{
        Burst: 5000, Rate: 1000, MaxInFlight: 200,
    },
})
defer ls.Stop()

listener, _ := net.Listen("unix", "/var/run/app.sock")
http.Serve(listener, ls.Handler(mux))
```

#### Unix socket behind a proxy that injects a client IP header

If a reverse proxy connects over a unix socket but injects the real client IP into a header, you can still use per-IP checks.

```go
ls := loadshedder.New(loadshedder.Config{
    PerIP: &loadshedder.PerIPConfig{
        Burst: 200, Rate: 50, MaxInFlight: 20,
        ProxyHeader: "X-Real-Ip",
    },
})
defer ls.Stop()

listener, _ := net.Listen("unix", "/var/run/app.sock")
http.Serve(listener, ls.Handler(mux))
```

For requests where the header is missing or contains an invalid IP (including whitespace, commas, or garbage), the middleware falls back to `RemoteAddr`. In proxy deployments this means the proxy's IP is used as the rate-limit key for that request — conservative (shared bucket) but not client-specific. On a unix socket this means those requests share a single bucket — which is typically fine since it only affects requests without a proper client IP.

## Performance

The hot path is zero-allocation: token consumption uses `atomic.AddInt64`, time reads use a coarse clock (`atomic.Int64` updated every 100ms), and per-IP state is stored in 64-shard maps with `RWMutex` per shard. Returning clients hit a read-only fast path (`RLock`) — write locks are only taken on the first request from a new IP. Concurrency limiting uses increment-then-validate-then-revert atomics — no TOCTOU window, no CAS retry loop.

Run benchmarks on your hardware:

```
go test -run=^$ -bench=. -benchmem -count=3
```

### Middleware overhead (no HTTP stack)

These use `discardResponseWriter` and pre-built requests to isolate the middleware cost from `net/http` and `httptest` allocations.

Results on Apple M4 Pro (12 cores, arm64):

| Benchmark | ns/op | reqs/s | allocs/op |
|---|---|---|---|
| MaxThroughput (multi-IP, parallel) | 83 | ~12,000,000 | 0 |
| MaxThroughput_SingleIP (parallel) | 188 | ~5,300,000 | 0 |
| HandlerOverhead (single-threaded) | 17 | — | 0 |
| ServerOnly (single-threaded) | 6 | — | 0 |
| WithTrustedProxyHeader (single-threaded) | 46 | — | 1 |
| TryTake (single-threaded) | 2 | — | 0 |

### Real-world HTTP (full stack)

These use `httptest.Server` with a real TCP listener over loopback, production-like config (server + per-IP limits, `X-Forwarded-For` header), HTTP/1.1 keep-alive connection pooling, and concurrent clients.

| Benchmark | reqs/s | allocs/op |
|---|---|---|
| APIBehindALB (per-IP + server + proxy header) | ~92,000 | 75 |
| APIBehindALB_ServerOnly (server-only) | ~101,000 | 52 |

The full-stack numbers are dominated by `net/http` and TCP overhead — the middleware itself adds negligible latency (see overhead benchmarks above).

## Design notes

**Concurrency limiting** uses an increment-then-validate-then-revert pattern (`atomic.Add` → check → `atomic.Add(-1)` on reject). This eliminates the TOCTOU window where multiple goroutines could simultaneously pass a load-check-then-increment sequence and exceed the limit by up to `GOMAXPROCS`.

**Token bucket refill** uses a timestamp-first CAS protocol: the refill goroutine claims a time window by CAS-ing the timestamp, then updates the token count in a separate CAS loop. This prevents double-crediting that could occur if the token CAS succeeded but the timestamp CAS failed.

**Cleanup safety** uses a two-phase mark-and-sweep: candidate keys are collected under read locks, then re-validated under write locks before deletion (via `DeleteIf`). This prevents deleting a bucket that was refreshed by a concurrent request between the scan and the delete.

**Cache-line isolation** pads the hot `requestCount` counter (incremented on every request) to its own 64-byte cache line, preventing false sharing with the shedded counters (only incremented on rejection).

## API reference

See the [godoc](https://pkg.go.dev/github.com/gurre/loadshedder) for detailed config struct documentation, validation rules, and per-field documentation.
