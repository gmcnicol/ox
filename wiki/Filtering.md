# Filtering

Filtering is processed inside the Ox broker, not in Oxtail.

Oxtail sends the filter expression. Ox owns compilation, validation, query planning, index use, and authoritative event filtering.

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

In v0, snapshot filtering can scan all current records for a topic and evaluate the CEL expression against each record.

With declared equality indexes:

```text
status == "OPEN"
  -> use idx/orders/status/OPEN/*
  -> fetch candidate records
  -> run full CEL filter to confirm
```

Even when an index is used, Ox should run the full filter as the final check.

## Watch filtering

The broker evaluates filters against old and new record values to produce correct transitions.

Client-side filtering is insufficient because it forces every client to receive all records, leaks data across filters, wastes bandwidth, and cannot reliably produce `leave` events unless the client sees every update for every key.
