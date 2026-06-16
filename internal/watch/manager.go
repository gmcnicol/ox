// Package watch manages the set of active WebSocket watchers for a topic.
//
// Design constraints:
//   - Slow consumers cannot block the topic write path.
//   - Watchers are removed immediately on disconnect.
//   - Each watcher has its own filter and send queue.
package watch

import (
	"sync"
	"sync/atomic"

	"github.com/gmcnicol/ox/internal/filter"
	"github.com/gmcnicol/ox/internal/topic"
)

const (
	// DefaultQueueDepth is the number of watch events buffered per watcher.
	// If the queue is full the watcher is considered slow and is dropped.
	DefaultQueueDepth = 256
)

// Watcher represents a single connected client subscription.
type Watcher struct {
	id     uint64
	filter *filter.Filter

	// send is the outbound queue for this watcher.
	// It is a buffered channel; the writer never blocks.
	send chan topic.WatchEvent

	// done is closed when the watcher should be torn down.
	done chan struct{}

	closeOnce sync.Once
}

// newWatcher allocates a watcher with the given compiled filter.
func newWatcher(id uint64, f *filter.Filter, queueDepth int) *Watcher {
	return &Watcher{
		id:     id,
		filter: f,
		send:   make(chan topic.WatchEvent, queueDepth),
		done:   make(chan struct{}),
	}
}

// Send enqueues an event.  If the queue is full the event is dropped and
// false is returned; the caller may choose to close the watcher.
func (w *Watcher) Send(ev topic.WatchEvent) bool {
	select {
	case w.send <- ev:
		return true
	default:
		return false // slow consumer
	}
}

// Events returns the read side of the watcher's send queue.
func (w *Watcher) Events() <-chan topic.WatchEvent { return w.send }

// Done returns a channel that is closed when the watcher has been torn down.
func (w *Watcher) Done() <-chan struct{} { return w.done }

// Close shuts down the watcher exactly once.
func (w *Watcher) Close() {
	w.closeOnce.Do(func() {
		close(w.done)
	})
}

// ID returns the unique watcher identifier.
func (w *Watcher) ID() uint64 { return w.id }

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

var nextID atomic.Uint64

// Manager holds all active watchers for a single topic and fans out events.
type Manager struct {
	mu       sync.RWMutex
	watchers map[uint64]*Watcher

	queueDepth int
}

// NewManager creates an empty Manager.
func NewManager(queueDepth int) *Manager {
	if queueDepth <= 0 {
		queueDepth = DefaultQueueDepth
	}
	return &Manager{
		watchers:   make(map[uint64]*Watcher),
		queueDepth: queueDepth,
	}
}

// Register creates and stores a new Watcher for the given filter expression.
func (m *Manager) Register(filterExpr string) (*Watcher, error) {
	f, err := filter.Compile(filterExpr)
	if err != nil {
		return nil, err
	}
	id := nextID.Add(1)
	w := newWatcher(id, f, m.queueDepth)

	m.mu.Lock()
	m.watchers[id] = w
	m.mu.Unlock()

	return w, nil
}

// Unregister removes the watcher and closes it.
func (m *Manager) Unregister(id uint64) {
	m.mu.Lock()
	w, ok := m.watchers[id]
	if ok {
		delete(m.watchers, id)
	}
	m.mu.Unlock()

	if ok {
		w.Close()
	}
}

// Dispatch evaluates ev against each watcher's filter and enqueues the
// appropriate WatchEvent.  Slow consumers (full queues) are collected and
// removed after the write lock is released to avoid holding the lock during
// cleanup.
//
// The old record (before the write) is needed to compute enter/update/leave.
// A nil oldValue means the key did not previously exist.
func (m *Manager) Dispatch(ev topic.Event, oldValue []byte) {
	m.mu.RLock()
	watchers := make([]*Watcher, 0, len(m.watchers))
	for _, w := range m.watchers {
		watchers = append(watchers, w)
	}
	m.mu.RUnlock()

	var slow []uint64

	for _, w := range watchers {
		we, skip := buildWatchEvent(w.filter, ev, oldValue)
		if skip {
			continue
		}
		if !w.Send(we) {
			// Queue full – mark for eviction.
			slow = append(slow, w.id)
		}
	}

	// Evict slow consumers.
	for _, id := range slow {
		m.Unregister(id)
	}
}

// buildWatchEvent computes which WatchEvent (if any) to emit for this watcher.
// Returns (event, skip=true) when the watcher does not care about this change.
func buildWatchEvent(f *filter.Filter, ev topic.Event, oldValue []byte) (topic.WatchEvent, bool) {
	var oldMatches, newMatches bool

	if oldValue != nil {
		var err error
		oldMatches, err = f.Match(oldValue)
		if err != nil {
			oldMatches = false
		}
	}

	if ev.Op == topic.OperationPut && ev.Value != nil {
		var err error
		newMatches, err = f.Match(ev.Value)
		if err != nil {
			newMatches = false
		}
	}

	// Transition table from the spec.
	switch {
	case ev.Op == topic.OperationDelete:
		if !oldMatches {
			return topic.WatchEvent{}, true
		}
		return topic.WatchEvent{
			Type:     topic.WatchEventDelete,
			Key:      ev.Key,
			Revision: ev.Revision,
		}, false

	case !oldMatches && !newMatches:
		return topic.WatchEvent{}, true

	case !oldMatches && newMatches:
		return topic.WatchEvent{
			Type:     topic.WatchEventEnter,
			Key:      ev.Key,
			Revision: ev.Revision,
			Value:    ev.Value,
		}, false

	case oldMatches && newMatches:
		return topic.WatchEvent{
			Type:     topic.WatchEventUpdate,
			Key:      ev.Key,
			Revision: ev.Revision,
			Value:    ev.Value,
		}, false

	case oldMatches && !newMatches:
		return topic.WatchEvent{
			Type:     topic.WatchEventLeave,
			Key:      ev.Key,
			Revision: ev.Revision,
		}, false
	}

	return topic.WatchEvent{}, true
}

// Count returns the number of active watchers (for metrics / tests).
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.watchers)
}
