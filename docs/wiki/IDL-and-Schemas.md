# IDL and schemas

## Decision

```text
IDL:              Protobuf
Protocol tooling: Buf
SDK transports:   gRPC for Go and JVM SDKs
Human API:         HTTP/JSON initially
Browser/simple:    WebSocket stream adapter
Schema registry:   repo-local first, Ox-native later
External registry: not required for v0
```

## Two schema layers

### 1. Ox protocol schema

This is the broker/client protocol:

- `PutRequest`
- `PutResponse`
- `DeleteRequest`
- `SnapshotRequest`
- `WatchRequest`
- `WatchEvent`
- `ReplayRequest`
- `TopicConfig`
- `Error`
- `Revision`

These live in `.proto` files and are versioned in Git.

Recommended layout:

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

### 2. User payload schema

This is the schema of records inside a topic.

For v0:

```text
record payload = JSON object
content_type   = application/json
filtering      = CEL over decoded JSON fields
schema         = optional / none
```

Do not force Protobuf payloads in v0.

Later:

```text
JSON Schema first
Protobuf payload schemas later
Avro only if bridge demand proves it necessary
```

## Protocol envelope sketch

```protobuf
syntax = "proto3";

package ox.v1;

option go_package = "github.com/gmcnicol/ox/gen/go/ox/v1;oxv1";
option java_package = "dev.ox.protocol.v1";
option java_multiple_files = true;

message Record {
  string topic = 1;
  string key = 2;
  uint64 revision = 3;
  bytes value = 4;
  string content_type = 5;
  string schema_id = 6;
}

enum Operation {
  OPERATION_UNSPECIFIED = 0;
  OPERATION_PUT = 1;
  OPERATION_DELETE = 2;
}

message Event {
  string topic = 1;
  string key = 2;
  uint64 revision = 3;
  Operation operation = 4;
  bytes value = 5;
  string content_type = 6;
  string schema_id = 7;
  int64 timestamp_unix_nano = 8;
}
```

## Watch service sketch

```protobuf
service OxService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Snapshot(SnapshotRequest) returns (stream SnapshotEvent);
  rpc Watch(WatchRequest) returns (stream WatchEvent);
  rpc Replay(ReplayRequest) returns (stream Event);
}
```

## Buf config sketch

`buf.yaml`:

```yaml
version: v2

lint:
  use:
    - STANDARD

breaking:
  use:
    - FILE
```

`buf.gen.yaml`:

```yaml
version: v2
clean: true

plugins:
  - local: protoc-gen-go
    out: gen/go
    opt:
      - paths=source_relative

  - local: protoc-gen-go-grpc
    out: gen/go
    opt:
      - paths=source_relative

  - protoc_builtin: java
    out: gen/java

  - local: protoc-gen-grpc-java
    out: gen/java
```

## Schema registry plan

### v0

Protocol schemas:

- Stored in Git under `proto/`
- Validated by Buf in CI

User payload schemas:

- None required
- JSON object payloads only
- `content_type = application/json`
- Filtering works on decoded JSON

### v1

Add an Ox-native schema catalog:

```text
POST /v1/schemas
GET  /v1/schemas/{id}
POST /v1/topics/{topic}/schema
```

CLI sketch:

```bash
ox schema register orders-v1 \
  --type jsonschema \
  --file schemas/orders.v1.schema.json

ox topic create orders \
  --key order_id \
  --schema orders-v1
```

### v2

Add Protobuf user payload support:

```bash
ox schema register orders-proto-v1 \
  --type protobuf \
  --file proto/acme/orders/v1/order.proto \
  --message acme.orders.v1.Order
```

Then records can carry:

```text
content_type = application/x-protobuf
schema_id    = orders-proto-v1
value        = binary protobuf bytes
```

Server-side filtering would decode through the registered descriptor before evaluating CEL.

## Compatibility rules

For Ox protocol `.proto` files:

- Never reuse field numbers.
- Reserve deleted field numbers.
- Reserve deleted field names.
- Only add optional/new fields.
- Do not change field meaning.
- Do not casually rename packages.
