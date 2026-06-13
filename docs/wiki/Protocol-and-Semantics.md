# Protocol and semantics

Ox is built around keyed topics, per-topic revisions, snapshots, watches, and replay.

## Core terms

| Term | Meaning |
|---|---|
| Topic | Named collection of keyed records |
| Key | Stable identity of a record within a topic |
| Revision | Monotonic per-topic position assigned to committed writes |
| Record | Latest value for a key |
| Event | Put/delete operation at a revision |
| Snapshot | Current matching records at a revision |
| Watch | Live changes after a revision |
| Replay | Historical events from a revision |

## Event model

```go
type Event struct {
    Topic        string
    Key          string
    Revision     uint64
    Operation    Operation
    Value        []byte
    ContentType  string
    SchemaID     string
    TimeUnixNano int64
}
```

## Watch event types

- `snapshot_begin`
- `record`
- `snapshot_end`
- `enter`
- `update`
- `leave`
- `delete`
- `error`

## Snapshot-and-watch contract

A client requests:

```text
watch topic where <filter> with snapshot
```

Ox must:

1. Register the watch boundary at committed revision `N`.
2. Return matching records from a consistent snapshot at `N`.
3. Emit `snapshot_end` with revision `N`.
4. Deliver every relevant committed change after `N`.

Stream shape:

```json
{"type":"snapshot_begin","revision":1842}
{"type":"record","key":"order-123","revision":1830,"value":{"status":"OPEN"}}
{"type":"snapshot_end","revision":1842}
{"type":"enter","key":"order-789","revision":1843,"value":{"status":"OPEN"}}
{"type":"leave","key":"order-123","revision":1844}
```

## Watch transition rules

For every committed update, Ox evaluates the filter against the old and new record state.

```text
old matches | new matches | emitted event
------------|-------------|--------------
false       | false       | none
false       | true        | enter
true        | true        | update
true        | false       | leave
true        | deleted     | delete
```

## Subscription modes

### Snapshot only

```bash
ox snapshot orders 'status == "OPEN"'
```

Returns current matching state and exits.

### Watch only

```bash
ox watch orders 'status == "OPEN"'
```

Starts receiving changes from now onwards. No initial state.

### Snapshot and watch

```bash
ox watch orders 'status == "OPEN"' --snapshot
```

Returns current matching state first, then streams changes.

## Ordering

For v0:

- Revisions are per topic.
- Ordering is guaranteed per topic.
- Cross-topic ordering is not guaranteed.
- Writes become visible only after they are committed to the local WAL and applied to the current-state projection.
