package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/authcreds"
)

// The host pushes the always-on engine's bearer token to a dedicated,
// host-only control endpoint (kept off the GraphQL surface). A valid POST
// populates the shared authcreds.Holder; anything malformed leaves it
// untouched so a bad push can't blank a working token.
func TestControlCredentials(t *testing.T) {
	creds := authcreds.NewHolder()
	ts := httptest.NewServer(controlCredentials(creds))
	defer ts.Close()
	url := ts.URL

	// Valid push.
	body := `{"token":"tok-abc","expiresAt":"2030-01-02T03:04:05Z"}`
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if got := creds.Token(); got != "tok-abc" {
		t.Fatalf("holder token = %q, want tok-abc", got)
	}
	if exp := creds.Get().ExpiresAt; exp.IsZero() {
		t.Fatal("expiresAt not parsed into holder")
	}

	assertMethodNotAllowed(t, url) // wrong method rejected, holder unchanged

	// Malformed JSON and empty token: rejected, prior token preserved.
	for _, bad := range []string{`{not json`, `{"token":""}`} {
		resp, err := http.Post(url, "application/json", strings.NewReader(bad))
		if err != nil {
			t.Fatalf("POST %q: %v", bad, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("POST %q status = %d, want 400", bad, resp.StatusCode)
		}
		if got := creds.Token(); got != "tok-abc" {
			t.Fatalf("bad push %q clobbered token: now %q", bad, got)
		}
	}
}

// /control/wake invokes the configured poke on a valid POST; a wrong
// method is rejected and does not.
func TestControlWake(t *testing.T) {
	poked := make(chan struct{}, 1)
	ts := httptest.NewServer(controlWake(func() {
		select {
		case poked <- struct{}{}:
		default:
		}
	}))
	defer ts.Close()
	url := ts.URL

	resp, err := http.Post(url, "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	select {
	case <-poked:
	case <-time.After(2 * time.Second):
		t.Fatal("poke not invoked")
	}

	assertMethodNotAllowed(t, url)
	select {
	case <-poked:
		t.Fatal("GET must not poke")
	default:
	}
}

// assertMethodNotAllowed checks the host-only control endpoints reject a
// non-POST with 405 (shared by the /control/* tests).
func assertMethodNotAllowed(t *testing.T, url string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET %s status = %d, want 405", url, resp.StatusCode)
	}
}
