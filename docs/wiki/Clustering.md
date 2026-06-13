# Clustering

Clustering should be added in phases. Do not start with full distributed consensus.

## Recommendation

```text
v0: single-node
v1: async WAL followers
v2: Raft-backed HA
v3: partitioned / multi-Raft
```

Not Redis. Not Kafka. Not NATS. Not SWIM as the commit protocol. Not home-grown consensus.

## v0: single-node

Single-node correctness comes first:

- Put/delete
- Snapshot
- Watch
- Snapshot-and-watch
- Replay
- Restart recovery

Clustering before these semantics are correct will only distribute broken behaviour.

## v1: async WAL followers

First clustering feature should be leader plus asynchronous followers.

```text
client writes -> leader
leader appends WAL
leader applies Pebble projection
leader streams WAL records to followers
followers append WAL
followers apply their own Pebble projection
```

This provides:

- Read replicas
- Hot standby
- Backup follower
- Basic disaster recovery

It does not provide:

- Quorum writes
- Strong HA writes
- Automatic failover without data-loss risk
- Linearizable reads from followers

Call this replication, not consensus.

Follower reconnect protocol:

```text
FOLLOW topic FROM revision=N
```

If the leader still has WAL from `N`, the follower catches up. If not, the follower needs a fresh snapshot.

## v2: Raft for HA

For real HA, use Raft, likely through `go.etcd.io/raft/v3`.

Write path in Raft mode:

```text
client write
  -> route to Raft leader
  -> propose event to Raft
  -> Raft replicates event to quorum
  -> committed event is applied to local WAL/Pebble
  -> watcher notifications emit after commit
  -> client receives ack
```

In Raft mode, the Raft log is the commit authority.

The local WAL still matters for local persistence and replay, but it does not decide cluster safety.

## Raft group layout

Start with:

```text
one Raft group per Ox cluster
```

Later:

```text
one Raft group per shard/topic group
```

Eventually:

```text
cluster
  shard-001 -> topics: orders, positions
  shard-002 -> topics: prices
  shard-003 -> topics: alerts, limits
```

Each shard has:

- Leader
- Followers
- Raft log
- WAL segments
- Pebble projection
- Watch registry

## Membership

Start with static membership.

```toml
[cluster]
id = "dev"
node_id = "node-a"

[[cluster.nodes]]
id = "node-a"
addr = "node-a:8081"

[[cluster.nodes]]
id = "node-b"
addr = "node-b:8081"

[[cluster.nodes]]
id = "node-c"
addr = "node-c:8081"
```

Dynamic membership comes later through learners:

```text
add learner
catch up learner
promote learner to voter
remove voter
```

## Discovery and failure detection

Use static config first.

Later, `memberlist`/SWIM can be used for:

- Discovery
- Liveness hints
- Gossip
- Operational visibility

But never for:

- Write commitment
- Leader election safety
- Data consistency

Split:

```text
Raft       = safety and commit
memberlist = discovery and gossip
```

## Client routing

Clients may connect to any node.

If the node is not leader for the topic/shard, it returns:

```json
{
  "error": "not_leader",
  "leader": "node-b",
  "leader_addr": "node-b:8080"
}
```

Start with redirects. Later, nodes can proxy to the leader.

## Reads

Read modes:

```text
stale       follower can serve, may lag
leader      default safe mode
linearizable later, leader confirms authority before serving
```

Do not claim linearizable reads until Raft integration is mature.

## Watches under clustering

Watches should bind to the leader for the relevant shard.

On leadership change, the client receives redirect or disconnects and resumes:

```text
watch orders where status == "OPEN" from revision=1842
```

Server response:

```text
resume accepted
```

or:

```text
revision too old; full snapshot required
```

Watch is resumable by revision. It is not magically immortal.

## Snapshot-and-watch under Raft

The clustered contract remains:

```text
snapshot at committed revision N
then every relevant committed change after N
```

Visibility rule:

```text
Do not expose a write in snapshot/watch until it is Raft-committed and applied.
```

Emission rule:

```text
Raft commit -> apply to WAL/Pebble -> evaluate watchers -> emit events
```

Never emit on proposal. Emit only on commit.

## Avoid early

- Multi-master writes
- CRDT merge semantics
- Global ordering across all topics
- Automatic shard rebalancing
- Dynamic membership from day one
- Cross-region quorum writes
- Building custom consensus
- Gossip for correctness
- Kafka/NATS as hidden cluster substrate
