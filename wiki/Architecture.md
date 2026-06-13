# Architecture

Ox is a single-binary broker in its first version.

```text
client
  |
  v
HTTP / gRPC / WebSocket API
  |
  v
Topic Manager
  |
  +--> Segmented append-only WAL
  +--> Pebble current-state projection
  +--> Watch engine
```

## Main components

### Topic manager

Coordinates keyed topics, per-topic revisions, ordered writes, and watcher notifications.

Proposed write path:

```text
client put/delete
  -> topic manager
  -> allocate per-topic revision
  -> append event to WAL
  -> apply event to Pebble projection
  -> notify watchers
  -> return revision
```

Writes are ordered per topic. Revisions are monotonic per topic in v0.

### WAL

The WAL is the source of truth.

```text
data/
  topics/
    {topic}/
      log/
        00000000000000000001.oxlog
        00000000000010000000.oxlog
```

Each WAL record should include:

- magic/version
- topic or topic identifier
- revision
- timestamp
- operation: put/delete
- key bytes
- value bytes
- checksum

The log supports replay, restart recovery, and future replication.

### Pebble projection

Pebble stores current state and indexes.

Proposed key layout:

```text
rec/{topic}/{key}                    -> latest value + revision
idx/{topic}/{field}/{value}/{key}    -> revision marker
meta/{topic}/revision                -> latest applied revision
meta/{topic}/config                  -> topic config
```

Pebble is not the source of truth. It is the current-state projection derived from the WAL.

### Watch engine

The watch engine owns live subscriptions. Each watcher has a compiled filter and a send queue.

For each committed update, Ox evaluates the old and new record state:

```text
old matches | new matches | event
------------|-------------|-------
false       | false       | none
false       | true        | enter
true        | true        | update
true        | false       | leave
true        | deleted     | delete
```

Events are emitted after the WAL append and state application are complete.
