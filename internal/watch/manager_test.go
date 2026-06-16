package watch_test

import (
	"testing"
	"time"

	internaltopic "github.com/gmcnicol/ox/internal/topic"
	"github.com/gmcnicol/ox/internal/watch"
)

func TestRegisterAndUnregister(t *testing.T) {
	mgr := watch.NewManager(16)

	w, err := mgr.Register("")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if mgr.Count() != 1 {
		t.Fatalf("want 1 watcher, got %d", mgr.Count())
	}

	mgr.Unregister(w.ID())
	if mgr.Count() != 0 {
		t.Fatalf("want 0 watchers after unregister, got %d", mgr.Count())
	}

	// Done channel must be closed after unregister.
	select {
	case <-w.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Done() not closed after Unregister")
	}
}

func TestDispatch_Enter(t *testing.T) {
	mgr := watch.NewManager(16)
	w, _ := mgr.Register("") // match-all

	ev := internaltopic.Event{
		Topic:    "orders",
		Key:      "ord-1",
		Revision: 1,
		Op:       internaltopic.OperationPut,
		Value:    []byte(`{"status":"OPEN"}`),
	}
	mgr.Dispatch(ev, nil) // nil oldValue → key is new

	select {
	case got := <-w.Events():
		if got.Type != internaltopic.WatchEventEnter {
			t.Fatalf("expected enter, got %q", got.Type)
		}
		if got.Key != "ord-1" {
			t.Fatalf("expected key ord-1, got %q", got.Key)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for event")
	}
}

func TestDispatch_Update(t *testing.T) {
	mgr := watch.NewManager(16)
	w, _ := mgr.Register("")

	old := []byte(`{"status":"OPEN"}`)
	ev := internaltopic.Event{
		Topic:    "orders",
		Key:      "ord-1",
		Revision: 2,
		Op:       internaltopic.OperationPut,
		Value:    []byte(`{"status":"PARTIAL"}`),
	}
	mgr.Dispatch(ev, old)

	select {
	case got := <-w.Events():
		if got.Type != internaltopic.WatchEventUpdate {
			t.Fatalf("expected update, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for update event")
	}
}

func TestDispatch_Leave(t *testing.T) {
	mgr := watch.NewManager(16)
	w, _ := mgr.Register("")

	old := []byte(`{"status":"OPEN"}`)
	// Simulate a key whose new value no longer matches a filter.
	// With match-all filter both old and new match → update.
	// To get leave we need a filter; use a tiny one that old matches but new doesn't.
	//
	// Instead we use a delete operation:
	ev := internaltopic.Event{
		Topic:    "orders",
		Key:      "ord-1",
		Revision: 3,
		Op:       internaltopic.OperationDelete,
	}
	mgr.Dispatch(ev, old)

	select {
	case got := <-w.Events():
		if got.Type != internaltopic.WatchEventDelete {
			t.Fatalf("expected delete, got %q", got.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for delete event")
	}
}

func TestDispatch_NoMatchNoEvent(t *testing.T) {
	mgr := watch.NewManager(16)
	w, _ := mgr.Register("")

	// op=delete but old value is nil → old does not match → no event.
	ev := internaltopic.Event{
		Topic:    "orders",
		Key:      "ord-unknown",
		Revision: 4,
		Op:       internaltopic.OperationDelete,
	}
	mgr.Dispatch(ev, nil) // old=nil, newMatches=false → skip

	select {
	case got := <-w.Events():
		t.Fatalf("unexpected event: %+v", got)
	case <-time.After(50 * time.Millisecond):
		// correct: no event expected
	}
}

func TestSlowConsumer_Eviction(t *testing.T) {
	queueDepth := 2
	mgr := watch.NewManager(queueDepth)
	w, _ := mgr.Register("")

	ev := internaltopic.Event{
		Topic:    "t",
		Key:      "k",
		Revision: 1,
		Op:       internaltopic.OperationPut,
		Value:    []byte(`{}`),
	}

	// Fill the queue and then overflow it.  The (queueDepth+1)-th dispatch
	// should trigger eviction.
	for i := 0; i <= queueDepth; i++ {
		mgr.Dispatch(ev, nil)
	}

	// The watcher should have been removed.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.Count() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if mgr.Count() != 0 {
		t.Fatal("slow consumer was not evicted")
	}

	// Done must be closed.
	select {
	case <-w.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("watcher.Done() not closed after slow-consumer eviction")
	}
}

func TestMultipleWatchers_IndependentQueues(t *testing.T) {
	mgr := watch.NewManager(16)
	w1, _ := mgr.Register("")
	w2, _ := mgr.Register("")

	ev := internaltopic.Event{
		Topic:    "t",
		Key:      "k",
		Revision: 1,
		Op:       internaltopic.OperationPut,
		Value:    []byte(`{}`),
	}
	mgr.Dispatch(ev, nil)

	recv := func(w *watch.Watcher, label string) {
		t.Helper()
		select {
		case <-w.Events():
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("%s: timed out waiting for event", label)
		}
	}
	recv(w1, "w1")
	recv(w2, "w2")
}
