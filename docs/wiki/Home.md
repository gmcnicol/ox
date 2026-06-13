# Ox design notes

Ox is a FOSS live-state broker inspired by AMPS SOW semantics.

It stores the latest value for each key, keeps an append-only history, and lets clients request a filtered snapshot of current state followed by live changes without gaps.

Ox is not an AMPS clone and is not wire-compatible with AMPS. The initial goal is to provide an open implementation of the snapshot-and-watch pattern for applications that need current state plus change streams.

## Core idea

Given a keyed topic:

1. Store the latest value for each key.
2. Keep an append-only history.
3. Let clients query the current state.
4. Let clients subscribe to changes after that query without gaps.

## Naming

- **Ox**: broker/server, CLI, protocol, repository.
- **Oxtail**: SDK/client libraries used by applications.

## Core stack

- Go
- Custom segmented append-only WAL
- Pebble current-state store
- CEL-Go content filters
- Protobuf IDL
- Buf tooling
- gRPC for Oxtail Go and JVM SDKs
- HTTP/JSON for human-facing API
- WebSocket adapter for simple/browser clients

## MVP scope

- Single-node broker
- Keyed topics
- Put/delete/get records
- Snapshot queries
- Watch streams
- Atomic snapshot-and-watch
- Replay from revision
- CEL filters
- CLI
- Recovery from WAL
- Basic metrics and tests

## Out of scope for MVP

- Kafka/NATS/Postgres as core dependencies
- Clustering/Raft
- AMPS wire compatibility
- Full queue semantics
- Views/aggregation
- External schema registry dependency

## Pages

- [Product positioning](Product-Positioning.md)
- [Architecture](Architecture.md)
- [Protocol and semantics](Protocol-and-Semantics.md)
- [Filtering](Filtering.md)
- [Oxtail SDK](Oxtail-SDK.md)
- [IDL and schemas](IDL-and-Schemas.md)
- [Clustering](Clustering.md)
- [Roadmap](Roadmap.md)
