// Package topic – in-memory store implementation.
package topic

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MemStore is a thread-safe in-memory implementation of Store.
// It is intentionally simple and suitable for tests and the local dev CLI.
type MemStore struct {
	mu      sync.RWMutex
	topics  map[string]*memTopic
	nextSub atomic.Uint64
}

type memTopic struct {
	mu       sync.RWMutex
	records  map[string]*Record
	revision uint64
	subs     map[uint64]chan<- Event
}

// NewMemStore creates an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{topics: make(map[string]*memTopic)}
}

func (s *MemStore) topicFor(name string) *memTopic {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[name]
	if !ok {
		t = &memTopic{
			records: make(map[string]*Record),
			subs:    make(map[uint64]chan<- Event),
		}
		s.topics[name] = t
	}
	return t
}

// fanOut delivers ev to every subscriber channel without holding the topic
// lock.  Each send is non-blocking (select/default): if the subscriber's
// buffer is full we drop the event on that channel.  Back-pressure is
// handled at the watcher queue level; a full eventCh means the fan-out
// goroutine is behind, and it will catch up or be evicted as a slow consumer.
//
// This avoids the goroutine-leak risk of the previous "go func() { ch <- ev }()"
// pattern, where an in-flight goroutine could block forever on a channel whose
// reader had already exited (e.g. after Unsubscribe).
func fanOut(subs []chan<- Event, ev Event) {
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
			// Subscriber's buffer is full; drop.  The buffered eventCh in
			// watch_handler is sized to DefaultQueueDepth so this only
			// triggers if a connection's fan-out goroutine is severely behind.
		}
	}
}

// Put writes a value and fans out an Event to all subscribers.
func (s *MemStore) Put(topicName, key string, value []byte) (uint64, error) {
	t := s.topicFor(topicName)
	t.mu.Lock()
	t.revision++
	rev := t.revision
	var oldValue []byte
	if old, ok := t.records[key]; ok && old.Value != nil {
		oldValue = append([]byte(nil), old.Value...)
	}
	newValue := append([]byte(nil), value...)
	t.records[key] = &Record{Key: key, Revision: rev, Value: newValue}
	ev := Event{Topic: topicName, Key: key, Revision: rev, Op: OperationPut, Value: newValue, OldValue: oldValue, Timestamp: time.Now()}
	subs := make([]chan<- Event, 0, len(t.subs))
	for _, ch := range t.subs {
		subs = append(subs, ch)
	}
	t.mu.Unlock()

	fanOut(subs, ev)
	return rev, nil
}

// Delete removes a key.
func (s *MemStore) Delete(topicName, key string) (uint64, error) {
	t := s.topicFor(topicName)
	t.mu.Lock()
	old, ok := t.records[key]
	if !ok {
		t.mu.Unlock()
		return 0, fmt.Errorf("key %q not found", key)
	}
	t.revision++
	rev := t.revision
	oldValue := append([]byte(nil), old.Value...)
	delete(t.records, key)
	ev := Event{Topic: topicName, Key: key, Revision: rev, Op: OperationDelete, OldValue: oldValue, Timestamp: time.Now()}
	subs := make([]chan<- Event, 0, len(t.subs))
	for _, ch := range t.subs {
		subs = append(subs, ch)
	}
	t.mu.Unlock()

	fanOut(subs, ev)
	return rev, nil
}

// GetSnapshot implements Store.
func (s *MemStore) GetSnapshot(topicName, filterExpr string) (*Snapshot, error) {
	t := s.topicFor(topicName)
	t.mu.RLock()
	defer t.mu.RUnlock()

	snap := &Snapshot{Revision: t.revision}
	for _, rec := range t.records {
		snap.Records = append(snap.Records, *rec)
	}
	return snap, nil
}

// Subscribe implements Store.
func (s *MemStore) Subscribe(topicName string, ch chan<- Event) uint64 {
	t := s.topicFor(topicName)
	id := s.nextSub.Add(1)
	t.mu.Lock()
	t.subs[id] = ch
	t.mu.Unlock()
	return id
}

// Unsubscribe implements Store.
func (s *MemStore) Unsubscribe(topicName string, id uint64) {
	s.mu.RLock()
	t, ok := s.topics[topicName]
	s.mu.RUnlock()
	if !ok {
		return
	}
	t.mu.Lock()
	delete(t.subs, id)
	t.mu.Unlock()
}
