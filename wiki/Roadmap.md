# Roadmap

## MVP

1. Bootstrap Go repository, MIT license, and CI.
2. Define event model, revisions, and protocol.
3. Implement segmented append-only WAL.
4. Implement Pebble current-state store.
5. Implement topic manager and write path.
6. Implement HTTP API.
7. Implement CEL-based filters.
8. Implement WebSocket watch streams.
9. Implement atomic snapshot-and-watch semantics.
10. Implement CLI.
11. Add recovery, durability, and restart tests.
12. Add metrics, logging, and basic benchmarks.
13. Add declared equality indexes.
14. Add documentation and runnable examples.
15. Define Oxtail SDK API and client semantics.

## Future

- Kafka/NATS/Postgres bridges.
- Ox-native schema catalog.
- Protobuf user payloads.
- Async WAL followers.
- Raft HA.
- Partitioned/multi-Raft clustering.
- SDKs beyond Go and JVM.
