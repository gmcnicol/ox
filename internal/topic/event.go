// Package topic defines the core event model and the TopicStore interface.
package topic

import "time"

// Operation is the kind of write applied to a key.
type Operation int

const (
	OperationPut    Operation = iota + 1
	OperationDelete           // no Value
)

// Event is an immutable record of a single committed write.
type Event struct {
	Topic     string
	Key       string
	Revision  uint64
	Op        Operation
	Value     []byte // nil for delete
	OldValue  []byte // previous value for put/delete transition semantics; nil if key did not exist
	Timestamp time.Time
}

// WatchEventType distinguishes the semantic role of a message sent to a watcher.
type WatchEventType string

const (
	WatchEventSnapshotBegin WatchEventType = "snapshot_begin"
	WatchEventRecord        WatchEventType = "record"
	WatchEventSnapshotEnd   WatchEventType = "snapshot_end"
	WatchEventEnter         WatchEventType = "enter"
	WatchEventUpdate        WatchEventType = "update"
	WatchEventLeave         WatchEventType = "leave"
	WatchEventDelete        WatchEventType = "delete"
	WatchEventError         WatchEventType = "error"
	WatchEventHeartbeat     WatchEventType = "heartbeat"
)

// WatchEvent is sent over the wire to a watcher.
type WatchEvent struct {
	Type     WatchEventType `json:"type"`
	Key      string         `json:"key,omitempty"`
	Revision uint64         `json:"revision,omitempty"`
	Value    []byte         `json:"value,omitempty"`
	Error    string         `json:"error,omitempty"`
}

// Record is the current state of a single key inside a topic.
type Record struct {
	Key      string
	Revision uint64
	Value    []byte
}

// Snapshot is a point-in-time consistent read.
type Snapshot struct {
	Revision uint64
	Records  []Record
}

// Store is the minimal interface that the watch / HTTP layer needs.
// A real implementation wraps Pebble; a test double can be a simple map.
type Store interface {
	// GetSnapshot returns matching records at the current committed revision.
	// The filter string is evaluated server-side; an empty string means "all".
	GetSnapshot(topic, filter string) (*Snapshot, error)

	// Subscribe registers a channel that receives every committed Event for
	// the named topic.  The caller must call Unsubscribe when done.
	Subscribe(topic string, ch chan<- Event) (id uint64)
	Unsubscribe(topic string, id uint64)
}
