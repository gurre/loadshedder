# loadshedder

[![Go Reference](https://pkg.go.dev/badge/github.com/gurre/loadshedder.svg)](https://pkg.go.dev/github.com/gurre/loadshedder)

Ultra performant HTTP middleware that rejects excess traffic before it overwhelms your server.

It does four things:

1. **Server rate limiting** — a single token bucket shared by all clients caps the server-wide QPS.
2. **Server concurrency limiting** — a single atomic counter caps the total number of in-flight requests.
3. **Per-IP rate limiting** — each client gets a token bucket that refills over time. When the bucket is empty, requests are rejected with `429 Too Many Requests`.
4. **Per-IP concurrency limiting** — tracks how many requests each client has in flight. When the limit is reached, new requests are rejected with `429 Too Many Requests`.

All four dimensions are independently configurable and composable — a request must pass all active checks. Server limits with value zero are disabled. Clients are identified by IP address.

## Install

```
go get github.com/gurre/loadshedder
```


```go
ls := loadshedder.New(
    loadshedder.WithServerBurst(1000),          // server-wide burst of 1000
    loadshedder.WithServerRatePerSecond(200),    // sustain 200 req/s total
    loadshedder.WithServerMaxInFlight(100),      // at most 100 concurrent total
    loadshedder.WithPerIPBurst(200),             // burst of 200 per client
    loadshedder.WithPerIPRatePerSecond(50),      // sustain 50 req/s per client
    loadshedder.WithPerIPMaxInFlight(20),        // at most 20 concurrent per client
)

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

| Option | What it controls |
|---|---|
| `WithServerBurst` | Max tokens in the server-wide bucket (burst size). Zero = disabled. |
| `WithServerRatePerSecond` | Sustained total request rate across all clients |
| `WithServerMaxInFlight` | Max concurrent requests across all clients. Zero = disabled. |
| `WithPerIPBurst` | Max tokens per client bucket (burst size) |
| `WithPerIPRatePerSecond` | Sustained request rate per client |
| `WithPerIPMaxInFlight` | Max concurrent requests per client |

Call `Stop` to shut down the background cleanup goroutine when you're done.

## Performance

The hot path is zero-allocation: token consumption uses `atomic.AddInt64`, time reads use a coarse clock (`atomic.Int64` updated every 100ms), and per-IP state is stored in 64-shard maps with `RWMutex` per shard. Returning clients hit a read-only fast path (`RLock`) — write locks are only taken on the first request from a new IP.

Run `BenchmarkLoadShedder_MaxThroughput` to measure peak throughput on your hardware:

```
go test -run=^$ -bench=MaxThroughput -benchmem -count=3
```

`BenchmarkLoadShedder_MaxThroughput` distributes requests across distinct IPs (best case). `BenchmarkLoadShedder_MaxThroughput_SingleIP` funnels all cores through one bucket (worst case).

Results on Apple M4 Pro (12 cores, arm64):

| Benchmark | ns/op | req/s | allocs/op |
|---|---|---|---|
| MaxThroughput (multi-IP) | 76 | ~13,000,000 | 0 |
| MaxThroughput_SingleIP | 183 | ~5,500,000 | 0 |
| HandlerOverhead (single-threaded) | 18 | ~56,000,000 | 0 |
| TryTake (single-threaded) | 2 | ~540,000,000 | 0 |
