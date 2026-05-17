package server_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	syncpkg "github.com/kubetail-org/kstack-app/sidecar/internal/sync"
	"github.com/kubetail-org/kstack-app/sidecar/server"
	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

// fakeStatus is a hand StatusSource: drives Status() / WatchStatus()
// without standing up a real engine + fake upstream.
type fakeStatus struct {
	mu   sync.Mutex
	cur  syncpkg.Status
	subs map[chan syncpkg.Status]struct{}
}

func newFakeStatus(s syncpkg.Status) *fakeStatus {
	return &fakeStatus{cur: s, subs: make(map[chan syncpkg.Status]struct{})}
}

func (f *fakeStatus) Status() syncpkg.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur
}

func (f *fakeStatus) WatchStatus() (<-chan syncpkg.Status, func()) {
	ch := make(chan syncpkg.Status, 4)
	f.mu.Lock()
	f.subs[ch] = struct{}{}
	f.mu.Unlock()
	return ch, func() {
		f.mu.Lock()
		if _, ok := f.subs[ch]; ok {
			delete(f.subs, ch)
			close(ch)
		}
		f.mu.Unlock()
	}
}

func (f *fakeStatus) push(s syncpkg.Status) {
	f.mu.Lock()
	f.cur = s
	for ch := range f.subs {
		select {
		case ch <- s:
		default:
		}
	}
	f.mu.Unlock()
}

func statusStack(t *testing.T, fs graph.StatusSource) string {
	t.Helper()
	srv := httptest.NewServer(server.NewHandlerWithResolver(&graph.Resolver{Sync: fs}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// syncStatus query maps the engine Status onto the GraphQL type: state
// stringified, timestamps as Unix-millis ints, RetryAt only while backing
// off.
func TestSyncStatusQueryMapsEngineStatus(t *testing.T) {
	synced := time.UnixMilli(1_700_000_000_000)
	retry := time.UnixMilli(1_700_000_030_000)
	url := statusStack(t, newFakeStatus(syncpkg.Status{
		State:        syncpkg.StateBackoff,
		LastError:    "cloud unreachable",
		LastSyncedAt: synced,
		RetryAt:      retry,
	}))

	raw := postGQL(t, url, "", `{"query":"{ syncStatus { state lastError lastSyncedAt retryAt } }"}`)
	var resp struct {
		Data struct {
			SyncStatus struct {
				State        string `json:"state"`
				LastError    string `json:"lastError"`
				LastSyncedAt int64  `json:"lastSyncedAt"`
				RetryAt      int64  `json:"retryAt"`
			} `json:"syncStatus"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	got := resp.Data.SyncStatus
	if got.State != "BACKOFF" || got.LastError != "cloud unreachable" {
		t.Fatalf("got %+v", got)
	}
	if got.LastSyncedAt != synced.UnixMilli() || got.RetryAt != retry.UnixMilli() {
		t.Fatalf("timestamps: got %+v", got)
	}
}

// RetryAt is zeroed unless the engine is actually backing off, even if a
// stale RetryAt lingers on the struct.
func TestSyncStatusRetryAtZeroWhenNotBackoff(t *testing.T) {
	url := statusStack(t, newFakeStatus(syncpkg.Status{
		State:   syncpkg.StateLive,
		RetryAt: time.UnixMilli(1_700_000_030_000),
	}))
	raw := postGQL(t, url, "", `{"query":"{ syncStatus { state retryAt lastSyncedAt } }"}`)
	if !strings.Contains(string(raw), `"retryAt":0`) || !strings.Contains(string(raw), `"state":"LIVE"`) {
		t.Fatalf("want live + retryAt 0, got %s", raw)
	}
}

// syncStatusWatch emits the current status immediately, then every
// transition.
func TestSyncStatusWatchSnapshotThenTransitions(t *testing.T) {
	fs := newFakeStatus(syncpkg.Status{State: syncpkg.StateConnecting})
	url := statusStack(t, fs)

	wsURL := "ws" + strings.TrimPrefix(url, "http") + "/graphql"
	dialer := websocket.Dialer{
		Subprotocols:     []string{"graphql-transport-ws"},
		HandshakeTimeout: 5 * time.Second,
	}
	conn, _, err := dialer.DialContext(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	mustWrite(t, conn, `{"type":"connection_init"}`)
	if got := mustReadType(t, conn); got != "connection_ack" {
		t.Fatalf("want connection_ack, got %q", got)
	}
	mustWrite(t, conn, `{"id":"1","type":"subscribe","payload":{"query":"subscription { syncStatusWatch { state } }"}}`)

	readState := func(want string) {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read %s: %v", want, err)
		}
		var msg struct {
			Type    string `json:"type"`
			Payload struct {
				Data struct {
					SyncStatusWatch struct {
						State string `json:"state"`
					} `json:"syncStatusWatch"`
				} `json:"data"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		if msg.Type != "next" || msg.Payload.Data.SyncStatusWatch.State != want {
			t.Fatalf("want %q, got %s", want, raw)
		}
	}

	readState("CONNECTING") // current snapshot first
	fs.push(syncpkg.Status{State: syncpkg.StateLive})
	readState("LIVE") // then transitions

	mustWrite(t, conn, `{"id":"1","type":"complete"}`)
}
