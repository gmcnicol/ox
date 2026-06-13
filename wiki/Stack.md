# Stack

## Core

```text
Language:       Go
Durable log:    custom append-only segmented WAL
Current state:  Pebble
Filters:        CEL-Go
IDL:            Protobuf
Tooling:        Buf
SDK transport:  gRPC
Human API:      HTTP/JSON
Live adapter:   WebSocket
CLI:            Cobra
Metrics:        Prometheus-compatible endpoint
Logging:        log/slog
```

## Core dependencies

- `github.com/cockroachdb/pebble`
- `github.com/google/cel-go` or successor import path
- `github.com/go-chi/chi/v5`
- `github.com/coder/websocket`
- `github.com/spf13/cobra`
- `google.golang.org/protobuf`
- `google.golang.org/grpc`
- `github.com/prometheus/client_golang`

## Explicitly not core dependencies

- Kafka
- NATS
- Redis
- Postgres
- Flink
- Materialize
- Kubernetes
- Raft
- RocksDB/cgo
- Rust

Kafka, NATS, and Postgres are optional bridges later, not foundations.
