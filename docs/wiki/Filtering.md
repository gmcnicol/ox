# Filtering

Filtering is processed inside the Ox broker, not in Oxtail.

Oxtail sends a filter expression. Ox owns compilation, validation, query planning, and event filtering.

```text
Oxtail SDK
  sends: topic="orders", where='status == "OPEN"'

Ox broker
  compiles filter
  uses indexes where possible
  evaluates records
  emits only matching snapshot/watch events
```

## Filter language

Use CEL for v0.

Examples:

```text
status == "OPEN"
desk == "FX" && qty > 1000000
symbol in ["EURUSD", "GBPUSD"]
```

## Snapshot filtering

For:

```bash
ox snapshot orders 'status == "OPEN" && desk == "FX"'
```

Flow:

```text
HTTP/gRPC API
  -> parse request
  -> compile CEL filter
  -> open Pebble snapshot at revision N
  -> find candidate records
  -> evaluate filter against each candidate
  -> return matching records
```

In v0, the simplest implementation is scan-and-evaluate:

```text
scan all records in topic
run CEL against each record
return matches
```

Later, declared equality indexes can reduce the candidate set:

```text
status == "OPEN"
  -> use idx/orders/status/OPEN/*
  -> fetch candidate records
  -> run full CEL expression to confirm
```

Even when an index is used, Ox should run the full filter expression as the final check.

## Watch filtering

Each watcher stores a compiled filter.

For each committed update, Ox compares old and new state:

```text
oldMatches := filter(oldRecord)
newMatches := filter(newRecord)
```

Then emits the correct result-set transition:

```text
old matches | new matches | event
------------|-------------|-------
false       | false       | none
false       | true        | enter
true        | true        | update
true        | false       | leave
true        | deleted     | delete
```

## Responsibility split

Ox broker:

- Authoritative filtering
- Snapshot filtering
- Watch filtering
- Enter/update/leave/delete semantics
- Index use

Oxtail SDK:

- Passes filter string and options
- Receives filtered events
- Helps maintain local state
- May optionally validate syntax early
- Does not decide authoritative matches

## Why not client-side filtering?

Client-side filtering would require every client to receive every update.

That is bad for:

- Bandwidth
- Sensitive data boundaries
- Latency
- Slow consumers
- Server-side permissioning later

Client-side filtering also cannot correctly produce `leave` events unless it sees every update and maintains old matching state.
