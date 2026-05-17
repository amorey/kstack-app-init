package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
	"github.com/kubetail-org/kstack-app/sidecar/internal/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/syncstore"
	"github.com/kubetail-org/kstack-app/sidecar/server"
	"github.com/kubetail-org/kstack-app/sidecar/server/graph"
)

// A cloud-failed updateSettings surfaces the error to the client AND
// persists the mutation; a subsequent successful Drain replays it and
// clears the queue.
func TestUpdateSettingsQueuesOnCloudFailure(t *testing.T) {
	cloudCalls := 0
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudCalls++
		http.Error(w, "upstream down", http.StatusBadGateway)
	}))
	defer cloudSrv.Close()

	q := mutationqueue.New(filepath.Join(t.TempDir(), "mutations.json"))
	r := &graph.Resolver{
		Cloud: cloud.New(cloudSrv.URL),
		Store: syncstore.NewStore[prefs.Settings](filepath.Join(t.TempDir(), "settings.json")),
		Hub:   prefs.NewHub(),
		Queue: q,
	}
	sidecar := httptest.NewServer(server.NewHandlerWithResolver(r))
	defer sidecar.Close()

	mutation := `{"query":"mutation($input: UpdateSettingsInput!) { updateSettings(input: $input) { placeholder } }",` +
		`"variables":{"input":{"placeholder":"offline-edit"}}}`
	raw := postGQL(t, sidecar.URL, "tok", mutation)

	// Client is told it failed (not silently 'ok').
	if !strings.Contains(string(raw), `"errors"`) {
		t.Fatalf("want a GraphQL error surfaced, got %s", raw)
	}
	if p, _ := q.Pending(); !p {
		t.Fatal("mutation not queued after cloud failure")
	}

	// A successful drain replays the queued mutation and clears it.
	var pushed string
	if err := q.Drain(context.Background(), func(_ context.Context, in cloud.UpdateInput) error {
		pushed = *in.Placeholder
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if pushed != "offline-edit" {
		t.Fatalf("drained input = %q", pushed)
	}
	if p, _ := q.Pending(); p {
		t.Fatal("queue not cleared after successful drain")
	}
	if cloudCalls == 0 {
		t.Fatal("cloud was never attempted")
	}
}
