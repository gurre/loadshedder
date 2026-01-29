# loadshedder

[![Go Reference](https://pkg.go.dev/badge/github.com/gurre/loadshedder.svg)](https://pkg.go.dev/github.com/gurre/loadshedder)

Ultra performant HTTP middleware that rejects excess traffic before it overwhelms your server.

It does two things:

1. **Rate limiting** — each client gets a token bucket that refills over time. When the bucket is empty, requests are rejected with `429 Too Many Requests`.
2. **Concurrency limiting** — tracks how many requests each client has in flight. When the limit is reached, new requests are rejected with `429 Too Many Requests`.

Clients are identified by IP address.

## Install

```
go get github.com/gurre/loadshedder
```


```go
ls := loadshedder.New(
    loadshedder.WithTokenBucketCapacity(200),          // burst of 200 requests per client
    loadshedder.WithTokenBucketRefillRatePerSecond(50), // sustain 50 req/s per client
    loadshedder.WithMaxRequestsInFlight(20),            // at most 20 concurrent per client
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
| `WithTokenBucketCapacity` | Max tokens per client bucket (burst size) |
| `WithTokenBucketRefillRatePerSecond` | Sustained request rate per client |
| `WithMaxRequestsInFlight` | Max concurrent requests per client |

Call `Stop` to shut down the background cleanup goroutine when you're done.

## Performance

The middleware is lock-free on the hot path: token consumption uses `atomic.AddInt64`, time reads use a coarse clock (`atomic.Int64` updated every 100ms), and per-IP state is stored in 64-shard maps with `RWMutex` per shard.

Run `BenchmarkLoadShedder_MaxThroughput` to measure peak throughput on your hardware:

```
go test -run=^$ -bench=MaxThroughput -benchmem -count=3
```

`BenchmarkLoadShedder_MaxThroughput` distributes requests across distinct IPs (best case). `BenchmarkLoadShedder_MaxThroughput_SingleIP` funnels all cores through one bucket (worst case).

Results on Apple M4 Pro (12 cores, arm64):

| Benchmark | ns/op | req/s | allocs/op |
|---|---|---|---|
| MaxThroughput (multi-IP) | 171 | ~5,800,000 | 1 |
| MaxThroughput_SingleIP | 173 | ~5,800,000 | 1 |
| HandlerOverhead (single-threaded) | 22 | ~45,000,000 | 1 |
| TryTake (single-threaded) | 2 | ~500,000,000 | 0 |
