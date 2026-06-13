# Oxtail SDK

Oxtail is the SDK/client layer for Ox.

Naming split:

- **Ox**: broker/server, CLI, protocol, repository.
- **Oxtail**: SDK/client libraries used by applications to snapshot current state and tail subsequent changes.

## Language order

1. Go SDK
2. Java/JVM SDK

## SDK responsibilities

Oxtail should make it straightforward for applications to:

- Connect to an Ox server
- Put/delete records
- Get records by key
- Run snapshot queries
- Start watch streams
- Start snapshot-and-watch streams
- Resume a watch from a revision when possible
- Maintain a local keyed map from watch events

## SDK non-goals

Oxtail should not:

- Hide the broker consistency model
- Pretend watches are magic durable subscriptions
- Hide `leave` versus `delete`
- Perform authoritative filtering client-side
- Swallow reconnect/resume failures silently

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

state := map[string]json.RawMessage{}

for event := range sub.Events() {
    switch event.Type {
    case oxtail.EventRecord, oxtail.EventEnter, oxtail.EventUpdate:
        state[event.Key] = event.Value
    case oxtail.EventLeave, oxtail.EventDelete:
        delete(state, event.Key)
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
        default -> {}
    }
});
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

## Reconnect semantics

The SDK can help reconnect, but it must be explicit.

Recommended model:

1. Track the last processed revision.
2. Attempt resume from that revision.
3. If the broker still has the WAL range, resume.
4. If the revision is too old, request a fresh snapshot-and-watch.

The application should be able to observe whether it resumed or resnapshotted.
