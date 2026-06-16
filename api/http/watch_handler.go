// Package http wires the Ox HTTP API endpoints, including the WebSocket
// watch stream at GET /v1/topics/{topic}/watch.
package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/go-chi/chi/v5"

	internaltopic "github.com/gmcnicol/ox/internal/topic"
	"github.com/gmcnicol/ox/internal/watch"
)

const (
	// heartbeatInterval is how often the server sends a heartbeat to idle clients.
	heartbeatInterval = 30 * time.Second

	// writeTimeout is the per-message deadline when writing to a WebSocket.
	writeTimeout = 5 * time.Second
)

// Handler is the HTTP handler group for the Ox API.
type Handler struct {
	store internaltopic.Store
	log   *slog.Logger

	// managersMu guards managers; each WebSocket connection is its own
	// goroutine so concurrent calls to Watch() race on this map.
	managersMu sync.Mutex
	managers   map[string]*watch.Manager // keyed by topic name
}

// NewHandler creates a Handler backed by the given store.
// managers is pre-populated so callers can inject per-topic Managers (e.g. in tests).
func NewHandler(store internaltopic.Store, managers map[string]*watch.Manager, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	if managers == nil {
		managers = make(map[string]*watch.Manager)
	}
	return &Handler{store: store, managers: managers, log: log}
}

// Routes registers all API routes onto r.
func (h *Handler) Routes(r chi.Router) {
	r.Get("/v1/topics/{topic}/watch", h.Watch)
}

// Watch handles GET /v1/topics/{topic}/watch?where=...&snapshot=false
//
// Query parameters:
//
//	where    – CEL filter expression (optional, default match-all)
//	snapshot – "true" to send current state before live events (default false)
func (h *Handler) Watch(w http.ResponseWriter, r *http.Request) {
	topicName := chi.URLParam(r, "topic")
	filterExpr := r.URL.Query().Get("where")
	wantSnapshot := r.URL.Query().Get("snapshot") == "true"

	// Upgrade to WebSocket.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Allow all origins for dev; tighten in production.
		InsecureSkipVerify: true,
	})
	if err != nil {
		h.log.Error("websocket accept", "err", err)
		return
	}
	defer conn.CloseNow() //nolint:errcheck

	ctx := conn.CloseRead(r.Context())

	// Obtain (or lazily create) the Manager for this topic.
	mgr := h.managerFor(topicName)

	// Subscribe to raw store events BEFORE registering the watcher and
	// BEFORE taking the snapshot.  This ordering ensures we never miss an
	// event between snapshot and live-stream:
	//
	//   subscribe → snapshot → register watcher → start fan-out
	//
	// Any event committed after Subscribe returns will land in eventCh, so
	// even if it races with GetSnapshot we will re-deliver it as a live
	// event.  The client deduplicates by revision.
	//
	// The channel is buffered to DefaultQueueDepth so that concurrent
	// store.Put goroutines are not serialised on this goroutine's scheduling.
	// Without buffering every Put blocks until the fan-out goroutine reads,
	// delaying the 257th dispatch long enough to miss a 1-second test
	// deadline under scheduler pressure.
	eventCh := make(chan internaltopic.Event, watch.DefaultQueueDepth*4)
	subID := h.store.Subscribe(topicName, eventCh)
	defer h.store.Unsubscribe(topicName, subID)

	// Register watcher after subscribing so the fan-out goroutine has a
	// valid Watcher to dispatch into.
	watcher, err := mgr.Register(filterExpr)
	if err != nil {
		h.sendError(ctx, conn, "invalid filter: "+err.Error())
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid filter")
		return
	}
	defer mgr.Unregister(watcher.ID())

	// Fan-out goroutine: drain eventCh and call Dispatch, which enqueues
	// WatchEvents into watcher.send.  Exits when the watcher is evicted
	// (slow consumer) or the client disconnects.
	go func() {
		for {
			select {
			case ev, ok := <-eventCh:
				if !ok {
					return
				}
				mgr.Dispatch(ev, ev.OldValue)
			case <-watcher.Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Snapshot phase — taken after Subscribe so no events are lost.
	if wantSnapshot {
		snap, err := h.store.GetSnapshot(topicName, filterExpr)
		if err != nil {
			h.sendError(ctx, conn, "snapshot error: "+err.Error())
			_ = conn.Close(websocket.StatusInternalError, "snapshot error")
			return
		}

		// snapshot_begin
		if err := h.writeJSON(ctx, conn, internaltopic.WatchEvent{
			Type:     internaltopic.WatchEventSnapshotBegin,
			Revision: snap.Revision,
		}); err != nil {
			return
		}

		for _, rec := range snap.Records {
			if err := h.writeJSON(ctx, conn, internaltopic.WatchEvent{
				Type:     internaltopic.WatchEventRecord,
				Key:      rec.Key,
				Revision: rec.Revision,
				Value:    rec.Value,
			}); err != nil {
				return
			}
		}

		// snapshot_end
		if err := h.writeJSON(ctx, conn, internaltopic.WatchEvent{
			Type:     internaltopic.WatchEventSnapshotEnd,
			Revision: snap.Revision,
		}); err != nil {
			return
		}
	}

	// Live event loop.
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-watcher.Done():
			h.sendError(ctx, conn, "disconnected: slow consumer")
			_ = conn.Close(websocket.StatusPolicyViolation, "slow consumer")
			return
		default:
		}

		select {
		case <-ctx.Done():
			return

		case <-watcher.Done():
			// Manager evicted this watcher (slow consumer).
			h.sendError(ctx, conn, "disconnected: slow consumer")
			_ = conn.Close(websocket.StatusPolicyViolation, "slow consumer")
			return

		case ev, ok := <-watcher.Events():
			if !ok {
				return
			}
			if err := h.writeJSON(ctx, conn, ev); err != nil {
				return
			}

		case <-heartbeat.C:
			if err := h.writeJSON(ctx, conn, internaltopic.WatchEvent{
				Type: internaltopic.WatchEventHeartbeat,
			}); err != nil {
				return
			}
		}
	}
}

// writeJSON writes a single JSON message to the WebSocket with a per-write timeout.
func (h *Handler) writeJSON(ctx context.Context, conn *websocket.Conn, v any) error {
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return wsjson.Write(wctx, conn, v)
}

// sendError sends an error event to the client.
func (h *Handler) sendError(ctx context.Context, conn *websocket.Conn, msg string) {
	_ = h.writeJSON(ctx, conn, internaltopic.WatchEvent{
		Type:  internaltopic.WatchEventError,
		Error: msg,
	})
}

// managerFor returns the Manager for topicName, creating one on first access.
// It is safe for concurrent use.
func (h *Handler) managerFor(topicName string) *watch.Manager {
	h.managersMu.Lock()
	defer h.managersMu.Unlock()
	if mgr, ok := h.managers[topicName]; ok {
		return mgr
	}
	mgr := watch.NewManager(watch.DefaultQueueDepth)
	h.managers[topicName] = mgr
	return mgr
}

// ---------------------------------------------------------------------------
// JSON helper (used only when we can't use wsjson directly)
// ---------------------------------------------------------------------------

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

var _ = mustJSON // keep linter happy until used
