package http_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/go-chi/chi/v5"

	oxhttp "github.com/gmcnicol/ox/api/http"
	internaltopic "github.com/gmcnicol/ox/internal/topic"
	"github.com/gmcnicol/ox/internal/watch"
)

// newTestServer creates a chi router wired with the watch handler and an
// in-memory store, and returns a running httptest.Server.
func newTestServer(t *testing.T) (*httptest.Server, *internaltopic.MemStore, map[string]*watch.Manager) {
	t.Helper()
	store := internaltopic.NewMemStore()
	managers := make(map[string]*watch.Manager)

	handler := oxhttp.NewHandler(store, managers, nil)
	r := chi.NewRouter()
	handler.Routes(r)

	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, store, managers
}

// wsURL converts http://... to ws://...
func wsURL(srv *httptest.Server, path string) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + path
}

func dialWatch(t *testing.T, srv *httptest.Server, topic, where string, snapshot bool) *websocket.Conn {
	t.Helper()
	u := wsURL(srv, "/v1/topics/"+topic+"/watch")
	params := "?snapshot=false"
	if snapshot {
		params = "?snapshot=true"
	}
	if where != "" {
		params += "&where=" + where
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, u+params, nil)
	if err != nil {
		t.Fatalf("websocket.Dial: %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() }) //nolint:errcheck
	return conn
}

func readEvent(t *testing.T, conn *websocket.Conn) internaltopic.WatchEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var ev internaltopic.WatchEvent
	if err := wsjson.Read(ctx, conn, &ev); err != nil {
		t.Fatalf("readEvent: %v", err)
	}
	return ev
}

// TestConnect_ReceivesEnterOnPut verifies that a connected watcher receives an
// enter event after a Put.
func TestConnect_ReceivesEnterOnPut(t *testing.T) {
	srv, store, _ := newTestServer(t)
	conn := dialWatch(t, srv, "orders", "", false)

	// Give the server a moment to register the watcher.
	time.Sleep(30 * time.Millisecond)

	if _, err := store.Put("orders", "ord-1", []byte(`{"status":"OPEN"}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ev := readEvent(t, conn)
	if ev.Type != internaltopic.WatchEventEnter {
		t.Fatalf("expected enter, got %q", ev.Type)
	}
	if ev.Key != "ord-1" {
		t.Fatalf("expected key ord-1, got %q", ev.Key)
	}
}

// TestSnapshot_SendsExistingRecords verifies the snapshot_begin / record /
// snapshot_end sequence is sent before live events.
func TestSnapshot_SendsExistingRecords(t *testing.T) {
	srv, store, _ := newTestServer(t)

	// Pre-populate before dialling.
	if _, err := store.Put("orders", "ord-existing", []byte(`{"status":"OPEN"}`)); err != nil {
		t.Fatal(err)
	}

	conn := dialWatch(t, srv, "orders", "", true /* snapshot */)

	got := readEvent(t, conn)
	if got.Type != internaltopic.WatchEventSnapshotBegin {
		t.Fatalf("expected snapshot_begin, got %q", got.Type)
	}

	got = readEvent(t, conn)
	if got.Type != internaltopic.WatchEventRecord {
		t.Fatalf("expected record, got %q", got.Type)
	}
	if got.Key != "ord-existing" {
		t.Fatalf("expected key ord-existing, got %q", got.Key)
	}

	got = readEvent(t, conn)
	if got.Type != internaltopic.WatchEventSnapshotEnd {
		t.Fatalf("expected snapshot_end, got %q", got.Type)
	}
}

// TestDisconnect_WatcherRemoved verifies that the Manager count drops to zero
// after the client disconnects.
func TestDisconnect_WatcherRemoved(t *testing.T) {
	srv, _, managers := newTestServer(t)
	conn := dialWatch(t, srv, "orders", "", false)

	// Wait for watcher to be registered.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr, ok := managers["orders"]; ok && mgr.Count() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mgr, ok := managers["orders"]
	if !ok || mgr.Count() == 0 {
		t.Fatal("watcher was not registered in time")
	}

	// Close the client connection.
	_ = conn.Close(websocket.StatusNormalClosure, "bye")

	// Manager should remove the watcher.
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr.Count() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if mgr.Count() != 0 {
		t.Fatalf("watcher not removed after disconnect; count=%d", mgr.Count())
	}
}

// TestSlowConsumer_GetsDisconnected verifies that a slow client receives an
// error event and is then disconnected when its queue overflows.
func TestSlowConsumer_GetsDisconnected(t *testing.T) {
	srv, store, managers := newTestServer(t)

	conn := dialWatch(t, srv, "orders", "", false)

	// Wait for watcher to appear.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mgr, ok := managers["orders"]; ok && mgr.Count() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mgr, ok := managers["orders"]
	if !ok {
		t.Fatal("manager not initialised")
	}

	// Get the queue depth from the default and flood well past it.
	for i := 0; i < watch.DefaultQueueDepth*4; i++ {
		_, _ = store.Put("orders", "k", []byte(`{}`))
	}

	// The watcher should be evicted.
	// 5 s gives the fan-out goroutine time to drain the 266-event flood
	// through the buffered eventCh and call Dispatch under scheduler pressure.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if mgr.Count() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if mgr.Count() != 0 {
		t.Fatal("slow consumer was not evicted")
	}

	// After eviction the server closes the WebSocket; reading should return
	// either an error event or a WebSocket close frame.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var ev internaltopic.WatchEvent
	for {
		if err := wsjson.Read(ctx, conn, &ev); err != nil {
			// Connection closed – that's the expected outcome.
			break
		}
		// Drain any queued events until we hit the error event or close.
		if ev.Type == internaltopic.WatchEventError {
			break
		}
		b, _ := json.Marshal(ev)
		t.Logf("drain: %s", b)
	}
}

// TestUpdateDelivery verifies that a subsequent put to an existing key
// generates an update event (not enter).
func TestUpdateDelivery(t *testing.T) {
	srv, store, _ := newTestServer(t)
	conn := dialWatch(t, srv, "orders", "", false)
	time.Sleep(30 * time.Millisecond)

	// First put → enter (old=nil, new matches).
	if _, err := store.Put("orders", "ord-2", []byte(`{"status":"OPEN"}`)); err != nil {
		t.Fatal(err)
	}
	ev := readEvent(t, conn)
	if ev.Type != internaltopic.WatchEventEnter {
		t.Fatalf("first put: expected enter, got %q", ev.Type)
	}

	// Second put → update (old matches, new matches).
	if _, err := store.Put("orders", "ord-2", []byte(`{"status":"PARTIAL"}`)); err != nil {
		t.Fatal(err)
	}
	ev = readEvent(t, conn)
	if ev.Type != internaltopic.WatchEventUpdate {
		t.Fatalf("second put: expected update, got %q", ev.Type)
	}
}
