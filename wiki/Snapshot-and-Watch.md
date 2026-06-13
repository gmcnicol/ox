# Snapshot and Watch Semantics

The central Ox contract is atomic snapshot-and-watch.

A client can ask:

```text
Watch topic T where predicate P, with snapshot
```

Ox must:

1. Establish a subscription boundary at revision `N`.
2. Return a consistent snapshot of matching records at `N`.
3. Emit `snapshot_end` with revision `N`.
4. Stream every relevant committed change after `N`.

## Why this matters

A separate query followed by a subscription can miss updates between the query and the subscription. A subscription followed by a query can create duplicates. Ox makes the boundary explicit.

## Event types

- `snapshot_begin`
- `record`
- `snapshot_end`
- `enter`
- `update`
- `leave`
- `delete`
- `error`

## Client local-state model

The client can maintain:

```text
map[key]record
```

Rules:

- `record`, `enter`, `update`: set `map[key] = value`.
- `leave`, `delete`: remove `map[key]`.
- `snapshot_end`: snapshot phase complete; subsequent events are live.

## Resume

Watches are resumable by revision when the server still has retained WAL history.

```text
watch orders where status == "OPEN" after_revision=1842
```

If the revision is too old, the server should require a fresh snapshot.
