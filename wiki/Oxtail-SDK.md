# Oxtail SDK

Oxtail is the official client/SDK layer for Ox.

- **Ox**: broker/server, CLI, wire protocol, repository.
- **Oxtail**: SDKs used by applications to snapshot current state and tail subsequent changes.

Initial SDK order:

1. Go.
2. Java/JVM.

## Client responsibilities

Oxtail should:

- Connect to an Ox server.
- Put/delete records.
- Get records by key.
- Run snapshot queries.
- Start watch streams.
- Start snapshot-and-watch streams.
- Resume watches from a revision when possible.
- Expose typed events.
- Make it easy to maintain local keyed state.

Oxtail should not hide consistency semantics. If reconnect requires a fresh snapshot, the SDK should make that explicit.

## Go API sketch

```go
client, err := oxtail.Dial(ctx, "http://localhost:8080")
if err != nil {
    return err
}
defer client.Close()

sub, err := client.Watch(ctx, "orders",
    oxtail.Where(`status == "OPEN"`),
    oxtail.WithSnapshot(),
)
if err != nil {
    return err
}

for event := range sub.Events() {
    switch event.Type {
    case oxtail.EventRecord, oxtail.EventEnter, oxtail.EventUpdate:
        // apply event.Value to local state
    case oxtail.EventLeave, oxtail.EventDelete:
        // remove event.Key from local state
    }
}
```

## JVM API sketch

```java
var client = OxtailClient.connect("http://localhost:8080");

var subscription = client.watch("orders",
    WatchOptions.builder()
        .where("status == \"OPEN\"")
        .snapshot(true)
        .build()
);

subscription.events().forEach(event -> {
    switch (event.type()) {
        case RECORD, ENTER, UPDATE -> state.put(event.key(), event.value());
        case LEAVE, DELETE -> state.remove(event.key());
    }
});
```

## Event types

- `snapshot_begin`
- `record`
- `snapshot_end`
- `enter`
- `update`
- `leave`
- `delete`
- `error`
