# Product positioning

Ox is a FOSS live-state broker inspired by AMPS SOW semantics.

The target use case is not generic event streaming. The target use case is:

> Give me the current matching state, then keep me updated without gaps.

## Relationship to AMPS

Ox is inspired by the AMPS pattern around SOW/query-and-subscribe:

| AMPS concept | Ox equivalent |
|---|---|
| SOW / State of the World | Current-state store per keyed topic |
| `sow` query | `snapshot` |
| `subscribe` | `watch` |
| `sow_and_subscribe` | Atomic snapshot-and-watch |
| Transaction log | Segmented append-only WAL |
| Bookmark replay | Replay from revision |
| Content filters | CEL filters |

Ox is not an AMPS clone, not wire-compatible with AMPS, and does not initially aim for full AMPS parity.

## What Ox competes on first

Ox v0 should compete on one sharp thing:

```text
snapshot current state
+ subscribe to changes
+ no race between the two
+ filtered result sets
+ local, simple, FOSS
```

## What Ox should not claim early

Avoid claiming early support for:

- Enterprise HA
- Multi-site replication
- Queue semantics
- Delta subscriptions
- Views and joins
- Entitlements
- AMPS protocol compatibility
- Managed/cloud deployment

## Why not just Kafka?

Kafka is primarily a durable distributed log. Ox is a live-state broker.

Kafka-style architecture often becomes:

```text
Kafka
+ compacted topics
+ stream processor
+ current-state store
+ HTTP query API
+ WebSocket update service
+ custom replay/resume code
```

Ox aims to provide the common current-state-plus-changes primitive directly.

## Why not just Redis plus a database?

Redis can hold state. A database can query state. A queue can emit events.

Ox is specifically about the boundary between these operations:

```text
snapshot at revision N
then every relevant change after N
```

The snapshot-and-watch contract is the product.
