# Ox

Ox is a FOSS live-state broker inspired by AMPS SOW semantics.

It stores the latest value for each key, keeps an append-only history, and lets clients request a filtered snapshot of current state followed by live changes without gaps.

Ox is not an AMPS clone and is not wire-compatible with AMPS. The first target is the core pattern: current state plus change streams.

## Names

- **Ox**: broker/server, CLI, protocol, repository.
- **Oxtail**: SDK/client libraries used by applications to snapshot current state and tail subsequent changes.

## Core contract

For a keyed topic, Ox supports:

1. Store the latest value for each key.
2. Keep an append-only history of changes.
3. Query the current state, optionally filtered.
4. Subscribe to changes after that query without gaps.

The flagship operation is snapshot-and-watch:

```text
snapshot current matching records at revision N
then stream every relevant committed change after N
```

Example:

```bash
ox watch orders 'status == "OPEN"' --snapshot
```

The client receives:

```json
{"type":"snapshot_begin","revision":1842}
{"type":"record","key":"order-123","revision":1830,"value":{"status":"OPEN"}}
{"type":"snapshot_end","revision":1842}
{"type":"enter","key":"order-789","revision":1843,"value":{"status":"OPEN"}}
{"type":"update","key":"order-789","revision":1844,"value":{"status":"PARTIAL"}}
{"type":"leave","key":"order-123","revision":1845}
```

## Non-goals for MVP

- AMPS wire compatibility.
- Kafka/NATS/Postgres/Redis as core dependencies.
- Clustering or Raft in v0.
- Cross-topic transactions.
- Distributed exactly-once claims.
- Full SQL engine.
- Enterprise entitlement model.

## Initial stack

- Go.
- Custom segmented append-only WAL.
- Pebble for current state and indexes.
- CEL-Go for content filters.
- Protobuf IDL and Buf tooling.
- gRPC for Oxtail Go/JVM SDKs.
- HTTP/JSON for human/manual API.
- WebSocket stream adapter for simple clients.
- Prometheus-compatible metrics.
