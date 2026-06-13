# Clustering Roadmap

Clustering is deferred until single-node semantics are correct.

## Proposed phases

```text
v0: single-node
v1: async WAL followers
v2: Raft-backed HA
v3: partitioned / multi-Raft
```

## v0: single node

Prove local correctness first:

- put/delete
- snapshot
- watch
- snapshot-and-watch
- replay
- restart recovery

## v1: async WAL followers

Leader streams committed WAL records to followers.

```text
client writes -> leader
leader appends WAL
leader applies Pebble projection
leader streams WAL records to followers
followers append WAL
followers apply their own Pebble projection
```

This supports read replicas, hot standby, and backup-style followers. It is replication, not consensus.

## v2: Raft HA

Use Raft for strongly consistent HA. Do not build home-grown consensus.

Write path in Raft mode:

```text
client write
  -> route to Raft leader
  -> propose event
  -> replicate to quorum
  -> commit
  -> apply to local WAL/Pebble
  -> notify watchers
  -> acknowledge client
```

Only emit watcher events after commit.

## Membership

Start with static membership. Dynamic membership and learners come later.

## Discovery

Static config first. Gossip/memberlist may be useful later for discovery and liveness hints, but not for commit safety.

## Watches under clustering

Watches bind to the leader for the relevant topic/shard. On leader change, the client should reconnect with its last seen revision.

If the server still has retained WAL from that revision, it can resume. Otherwise, the client must request a new snapshot.
