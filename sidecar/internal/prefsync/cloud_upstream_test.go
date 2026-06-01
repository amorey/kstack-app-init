package prefsync_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/authcreds"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefsync"
)

// Compile-time proof the adapter satisfies the engine's Upstream.
var _ prefsync.Upstream = (*prefsync.CloudUpstream)(nil)

// No credentials yet (the host hasn't pushed): the adapter must error
// *without* touching the cloud, so the engine just goes Offline instead of
// hammering an unauthenticated endpoint.
func TestCloudUpstreamErrsWhenNoCredentials(t *testing.T) {
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hit = true
	}))
	defer srv.Close()

	up := prefsync.NewCloudUpstream(cloud.New(srv.URL), authcreds.NewHolder())

	if _, err := up.Snapshot(context.Background()); err == nil {
		t.Fatal("Snapshot: want error with empty holder, got nil")
	}
	if _, err := up.Watch(context.Background()); err == nil {
		t.Fatal("Watch: want error with empty holder, got nil")
	}
	if hit {
		t.Fatal("cloud was contacted despite no credentials")
	}
}

// With a token in the holder, Snapshot delegates to cloud.GetSettings and
// forwards the bearer.
func TestCloudUpstreamSnapshotForwardsToken(t *testing.T) {
	var sawAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"data":{"settings":{"placeholder":"from-cloud"}}}`))
	}))
	defer srv.Close()

	creds := authcreds.NewHolder()
	creds.Set(authcreds.Credentials{Token: "tok-1"})
	up := prefsync.NewCloudUpstream(cloud.New(srv.URL), creds)

	got, err := up.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got.Placeholder != "from-cloud" {
		t.Fatalf("Snapshot returned %+v", got)
	}
	if sawAuth != "Bearer tok-1" {
		t.Fatalf("bearer not forwarded: %q", sawAuth)
	}
}

// Watch opens the upstream SSE with the held token and surfaces events.
func TestCloudUpstreamWatchStreamsEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			_, _ = w.Write([]byte(`{"data":{}}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer tok-2" {
			http.Error(w, "bad auth", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fl.Flush()
		fmt.Fprint(w, "event: next\ndata: {\"data\":{\"settingsWatch\":{\"placeholder\":\"streamed\"}}}\n\n")
		fl.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	creds := authcreds.NewHolder()
	creds.Set(authcreds.Credentials{Token: "tok-2"})
	up := prefsync.NewCloudUpstream(cloud.New(srv.URL), creds)

	// Explicit cancel, not t.Context(): defers are LIFO, so this cancel
	// runs before `defer srv.Close()`. With t.Context() (cancelled only
	// after all defers) srv.Close() would deadlock waiting on the SSE
	// handler that's blocked until the client connection drops.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := up.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	select {
	case s, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before any event")
		}
		if s.Placeholder != "streamed" {
			t.Fatalf("event = %+v", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event within timeout")
	}
}
