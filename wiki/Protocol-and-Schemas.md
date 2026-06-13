# Protocol and Schemas

## Decision

- IDL: Protobuf.
- Protocol tooling: Buf.
- SDK transport: gRPC for Go and JVM SDKs.
- Human/manual API: HTTP/JSON initially.
- Simple stream adapter: WebSocket.
- Protocol schema registry: repository-local via Git + Buf.
- User payload schema registry: none in v0, Ox-native later.

## Two schema categories

### Ox protocol schema

Broker/client protocol messages:

- `PutRequest`
- `PutResponse`
- `DeleteRequest`
- `GetRequest`
- `SnapshotRequest`
- `WatchRequest`
- `WatchEvent`
- `ReplayRequest`
- `TopicConfig`
- `Error`
- `Revision`

These live under `proto/` and are versioned in Git.

Proposed layout:

```text
proto/
  buf.yaml
  buf.gen.yaml
  ox/
    v1/
      topic.proto
      record.proto
      watch.proto
      query.proto
      schema.proto
      admin.proto
```

### User payload schema

The schema of values inside topics.

For v0:

```text
record payload = JSON object
content_type   = application/json
filtering      = CEL over decoded JSON fields
schema         = optional / none
```

Do not require an external schema registry to run Ox.

## Later schema catalog

A future Ox-native schema catalog could support:

```http
POST /v1/schemas
GET  /v1/schemas/{id}
POST /v1/topics/{topic}/schema
```

CLI example:

```bash
ox schema register orders-v1 \
  --type jsonschema \
  --file schemas/orders.v1.schema.json

ox topic create orders \
  --key order_id \
  --schema orders-v1
```

Later, Protobuf user payloads can be supported by registering descriptors and message names.

## Compatibility rules

For `.proto` files:

- Never reuse field numbers.
- Reserve deleted field numbers.
- Reserve deleted field names.
- Only add optional/new fields.
- Do not change field meaning.
- Do not casually rename packages.
