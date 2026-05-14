package cloud_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
)

// GetSettings POSTs a `{ settings { placeholder } }` query with the bearer
// token in the Authorization header and decodes the GraphQL response.
func TestGetSettings(t *testing.T) {
	var sawAuth string
	var sawBody []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth = r.Header.Get("Authorization")
		sawBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"settings":{"placeholder":"hi"}}}`))
	}))
	defer ts.Close()

	c := cloud.New(ts.URL)
	got, err := c.GetSettings(context.Background(), "tok-123")
	if err != nil {
		t.Fatalf("GetSettings: %v", err)
	}
	if got != (prefs.Settings{Placeholder: "hi"}) {
		t.Fatalf("got %+v", got)
	}
	if sawAuth != "Bearer tok-123" {
		t.Fatalf("auth header: %q", sawAuth)
	}
	if !strings.Contains(string(sawBody), "settings") || !strings.Contains(string(sawBody), "placeholder") {
		t.Fatalf("body did not look like a settings query: %s", sawBody)
	}
}

// GraphQL errors surface as Go errors so the resolver layer can fall back
// to the local cache.
func TestGetSettingsGraphQLError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"errors":[{"message":"nope"}]}`))
	}))
	defer ts.Close()

	c := cloud.New(ts.URL)
	if _, err := c.GetSettings(context.Background(), "t"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// UpdateSettings posts a mutation carrying the input variables and returns
// the cloud's view of the resulting Settings.
func TestUpdateSettings(t *testing.T) {
	var sawBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&sawBody)
		_, _ = w.Write([]byte(`{"data":{"updateSettings":{"placeholder":"world"}}}`))
	}))
	defer ts.Close()

	c := cloud.New(ts.URL)
	got, err := c.UpdateSettings(context.Background(), "t", cloud.UpdateInput{Placeholder: strPtr("world")})
	if err != nil {
		t.Fatalf("UpdateSettings: %v", err)
	}
	if got != (prefs.Settings{Placeholder: "world"}) {
		t.Fatalf("got %+v", got)
	}
	vars, _ := sawBody["variables"].(map[string]any)
	input, _ := vars["input"].(map[string]any)
	if input["placeholder"] != "world" {
		t.Fatalf("input not forwarded as variables: %+v", sawBody)
	}
}

// WatchSettings opens an SSE stream and emits each `next` event's data
// payload on the returned channel, in order. Cancelling the context closes
// the channel.
func TestWatchSettings(t *testing.T) {
	emit := make(chan string, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Errorf("Accept header: %q", r.Header.Get("Accept"))
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("Authorization: %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		flusher.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case v, ok := <-emit:
				if !ok {
					fmt.Fprint(w, "event: complete\ndata: \n\n")
					flusher.Flush()
					return
				}
				fmt.Fprintf(w, "event: next\ndata: {\"data\":{\"settingsWatch\":{\"placeholder\":%q}}}\n\n", v)
				flusher.Flush()
			}
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := cloud.New(ts.URL)
	ch, err := c.WatchSettings(ctx, "tok")
	if err != nil {
		t.Fatalf("WatchSettings: %v", err)
	}

	emit <- "one"
	emit <- "two"

	for _, want := range []string{"one", "two"} {
		select {
		case got, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed before %q", want)
			}
			if got.Placeholder != want {
				t.Fatalf("got %+v want placeholder=%q", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %q", want)
		}
	}

	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			// drain any in-flight, then expect close
			select {
			case _, ok2 := <-ch:
				if ok2 {
					t.Fatal("channel still emitting after cancel")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("channel did not close after cancel")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel did not close after cancel")
	}
}

func strPtr(s string) *string { return &s }
