# Roadmap

## MVP goal

Build Ox as a single-node live-state broker that stores the latest value for each key, keeps an append-only history, and supports snapshot-and-watch semantics without gaps.

## MVP scope

- Single Go binary
- Keyed topics
- Append-only segmented WAL
- Pebble-backed current-state store
- Monotonic per-topic revisions
- HTTP API for put/get/delete/snapshot/history
- gRPC protocol for Oxtail SDKs
- WebSocket watch adapter
- Atomic snapshot-and-watch
- CEL-based content filters
- CLI
- Recovery on restart
- Basic metrics

## Explicitly out of scope for MVP

- Kafka/NATS/Postgres as core dependencies
- Clustering/Raft
- Cross-topic transactions
- Distributed exactly-once claims
- Advanced SQL engine
- Advanced secondary indexes beyond declared equality indexes
- Managed/cloud deployment

## Implementation order

1. Bootstrap Go repository and CI
2. Define event model, revisions, and protocol docs
3. Add Protobuf IDL and Buf generation
4. Implement segmented WAL
5. Implement Pebble current-state projection
6. Implement topic manager and write path
7. Implement HTTP API
8. Implement CEL filters
9. Implement WebSocket watch streams
10. Implement atomic snapshot-and-watch correctness
11. Implement Oxtail Go SDK
12. Implement CLI
13. Add recovery, restart tests, and durability documentation
14. Add metrics and benchmarks
15. Add declared equality indexes
16. Add Oxtail JVM SDK
17. Add docs and runnable examples

## Current GitHub issues

- #1 Roadmap: MVP live-state broker
- #2 Bootstrap Go repository, MIT license, and CI
- #3 Define event model, revisions, and wire protocol
- #4 Implement segmented append-only WAL
- #5 Implement Pebble current-state store
- #6 Implement topic manager and write path
- #7 Implement HTTP API for topics, records, snapshots, and history
- #8 Implement CEL-based content filters
- #9 Implement WebSocket watch streams
- #10 Implement atomic snapshot-and-watch semantics
- #11 Implement CLI for local development and manual testing
- #12 Add recovery, durability, and restart tests
- #13 Add metrics, logging, and basic benchmarking
- #14 Add declared equality indexes for snapshot and watch filters
- #15 Add documentation and runnable examples
- #16 Future: Kafka, NATS, and Postgres bridges
- #17 Future: replication and clustering design
- #18 Define Oxtail SDK API and client semantics

## Deferred/future

### Bridges

- Kafka import/export
- NATS import/export
- Postgres import, likely CDC-oriented

These are connectors, not core dependencies.

### Replication and clustering

- Async WAL followers
- Raft-backed HA
- Sharded/multi-Raft deployment
- Learner-based membership changes
- Watch resume across failover

### Native schema catalog

- JSON Schema payload validation
- Protobuf payload schemas
- Schema IDs on records
- Descriptor-based decoding for server-side filters
