# Architecture

Ox is a single Go daemon with an embedded durable log and current-state store.

```text
client
  │
  ▼
HTTP / gRPC / WebSocket API
  │
  ▼
Topic manager
  │
  ├── WAL segments          source of truth
  ├── Pebble store          current-state projection
  ├── filter engine         CEL compile/evaluate
  └── watch engine          live result-set changes
```

## Core technologies

| Layer | Technology |
|---|---|
| Language | Go |
| Durable log | Custom append-only segmented WAL |
| Current state | Pebble |
| Filters | CEL-Go |
| IDL | Protobuf |
| Protocol tooling | Buf |
| SDK transport | gRPC |
| Human API | HTTP/JSON |
| Simple live stream | WebSocket adapter |
| CLI | Go CLI |
| Metrics | Prometheus-compatible endpoint |

## Core storage model

```text
WAL     = what happened
Pebble  = what is true now
Watch   = what changed after a revision
```

The WAL is the source of truth. Pebble is a projection rebuilt from the WAL if needed.

## Per-topic write path

Each topic has an ordered write path.

```text
put/delete request
  -> allocate per-topic revision
  -> append event to WAL
  -> apply event to Pebble projection
  -> evaluate and notify watchers
  -> return revision
```

The first version guarantees ordering per topic, not global ordering across all topics.

## Data layout

Example layout:

```text
data/
  topics/
    orders/
      log/
        00000000000000000001.oxlog
        00000000000010000000.oxlog
      state/
        pebble files...
```

Example Pebble keys:

```text
rec/{topic}/{key}                    -> latest value + revision
idx/{topic}/{field}/{value}/{key}    -> index marker
meta/{topic}/revision                -> latest applied revision
meta/{topic}/config                  -> topic config
```

## Dependencies deliberately avoided in the core

Ox v0 should not depend on:

- Kafka
- NATS
- Redis
- Postgres
- Flink
- Materialize
- Kubernetes
- Raft

These can exist as bridges or later deployment options, but not as core requirements.
