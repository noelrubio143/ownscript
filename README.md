# goproxy — minimal HAProxy-style TCP load balancer (Go)

A single-binary, no-dependency TCP reverse proxy / connection multiplexer,
similar in spirit to HAProxy running in `mode tcp`.

## Features

- Layer 4 (raw TCP) proxying — works with any TCP protocol (HTTP, databases,
  custom binary protocols, etc.)
- Multiple independent listeners ("frontends"), each with its own backend pool
- Load balancing: `roundrobin` (weighted) or `leastconn`
- Active health checks (TCP dial every N seconds, 3 consecutive failures = DOWN,
  auto re-enable once reachable again)
- Per-connection full-duplex multiplexing via `io.Copy` goroutines
- JSON stats endpoint (`GET /stats`) showing backend health + active connections
- Graceful shutdown on SIGINT/SIGTERM

## Build

```bash
go build -o goproxy .
```

## Run

```bash
./goproxy -config config.json
```

## Config format (`config.json`)

```jsonc
{
  "stats_addr": ":8404",          // optional stats HTTP endpoint
  "listeners": [
    {
      "name": "web_frontend",
      "bind": ":8080",             // where clients connect
      "balance": "roundrobin",     // or "leastconn"
      "check_interval_sec": 5,
      "check_timeout_sec": 2,
      "connect_timeout_sec": 3,
      "backends": [
        { "addr": "127.0.0.1:9001", "weight": 1 },
        { "addr": "127.0.0.1:9002", "weight": 2 }
      ]
    }
  ]
}
```

## Checking stats

```bash
curl http://localhost:8404/stats | jq
```

## What this does NOT do (yet)

- No TLS termination/SNI routing (add via `crypto/tls` if needed — happy to
  extend this to Layer 7 / SNI-based routing like HAProxy's `req.ssl_sni`)
- No weighted reload without restart (SIGHUP currently just re-reads the file;
  wiring it to swap pools live is a small follow-up)
- No HTTP-aware (Layer 7) routing — this is pure TCP passthrough

## Extending it

The pieces to touch:
- `Pool.Pick()` — add new balancing algorithms here
- `Pool.checkOne()` — swap TCP dial check for an HTTP health-check request
- `handleConn()` — add TLS SNI inspection before picking a backend if you want
  SNI-based routing (peek the ClientHello without terminating TLS, like Cloak/
  HAProxy's `tcp-request inspect-delay` + `req.ssl_sni`)
