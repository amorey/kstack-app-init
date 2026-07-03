package graph_test

// Behavioral tests for the resolvers in schema.resolvers.go, exercised over a
// real gqlgen HTTP server (POST for queries/mutations, SSE for subscriptions).
// The SSE plumbing helpers (openSSESubscription/sseEvents/nextSSE) live in
// server_test.go alongside the transport canaries — they're shared within this
// package.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
)

// postGQL POSTs a GraphQL query/mutation body to url's /graphql endpoint and
// returns the raw response.
func postGQL(t *testing.T, url, body string) []byte {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return raw
}

// --- cloud account: authState / login / logout / authStateWatch -----------
//
// The fakes (memCredStore/fakeOAuthFlow/fakeLoopback) live in testutils_test.go;
// the postGQL / SSE helpers are defined above + in server_test.go.

// The authState query reflects a signed-out auth service (no stale identity).
func TestAuthStateQuerySignedOut(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: newFakeAuth(auth.Identity{})}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"{ authState { authenticated identity { sub } } }"}`))
	if !strings.Contains(raw, `"identity":null`) {
		t.Fatalf("want signed-out auth state (null identity), got: %s", raw)
	}
	if !strings.Contains(raw, `"authenticated":false`) {
		t.Fatalf("want authenticated false, got: %s", raw)
	}
}

// signedInAuth returns a fake auth.Service already signed in as the given
// identity. The resolver depends on the auth.Service interface, so the tests fake
// it (see fakeAuth) rather than constructing the real service.
func signedInAuth(t *testing.T, id auth.Identity) auth.Service {
	t.Helper()
	return signedInFakeAuth(id)
}

// configuredAuth returns a signed-out fake auth.Service that signs in as id when
// Login runs (the resolver's login flow).
func configuredAuth(t *testing.T, id auth.Identity) auth.Service {
	t.Helper()
	return newFakeAuth(id)
}

// The authState query reflects the current signed-in identity.
func TestAuthStateQuerySignedIn(t *testing.T) {
	svc := signedInAuth(t, auth.Identity{UserID: "u1", Email: "a@x.com", Name: "Ada"})

	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"{ authState { authenticated identity { sub email name } } }"}`))
	if !strings.Contains(raw, `"email":"a@x.com"`) {
		t.Fatalf("want signed-in identity, got: %s", raw)
	}
	if !strings.Contains(raw, `"authenticated":true`) {
		t.Fatalf("want authenticated true, got: %s", raw)
	}
}

// authStateWatch emits the current snapshot first, then a fresh snapshot on change.
func TestAuthStateWatchSnapshotThenDelta(t *testing.T) {
	svc := configuredAuth(t, auth.Identity{Email: "a@x.com"})
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	resp := openSSESubscription(t, srv.URL, "",
		"subscription { authStateWatch { authenticated identity { email } } }")
	defer resp.Body.Close() // ends the subscription; must run before srv.Close()
	events := sseEvents(t, resp)

	if ev := nextSSE(t, events); ev.event != "next" || !strings.Contains(ev.data, `"authenticated":false`) {
		t.Fatalf("first frame: event=%q data=%s", ev.event, ev.data)
	}

	if err := svc.StartLogin(context.Background()); err != nil {
		t.Fatalf("Login: %v", err)
	}

	if ev := nextSSE(t, events); ev.event != "next" || !strings.Contains(ev.data, `"email":"a@x.com"`) {
		t.Fatalf("delta frame: event=%q data=%s", ev.event, ev.data)
	}
}

// logout delegates to the account: returns true and flips the session signed-out.
func TestLogoutMutation(t *testing.T) {
	svc := signedInFakeAuth(auth.Identity{})
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { authLogout }"}`))
	if !strings.Contains(raw, `"authLogout":true`) {
		t.Fatalf("want authLogout true, got: %s", raw)
	}
	if cur, _ := svc.Current(context.Background()); cur.Authenticated {
		t.Fatal("session still signed in after logout")
	}
}

// login is non-blocking: it returns true immediately and the resulting signed-in
// session arrives asynchronously (observed here via the auth-state watch), proving
// the mutation kicked off the flow without blocking on the browser round-trip.
func TestLoginMutationKicksOffFlow(t *testing.T) {
	svc := newFakeAuth(auth.Identity{Email: "a@x.com"})

	// The auth-state stream is latest-value (current-on-subscribe): the first State
	// is the signed-out baseline, and the signed-in flow surfaces as a later State
	// with Authenticated true. The loop skips the baseline and waits for sign-in.
	states, cancel := svc.Subscribe()
	defer cancel()

	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { authLoginStart }"}`))
	if !strings.Contains(raw, `"authLoginStart":true`) {
		t.Fatalf("want login true, got: %s", raw)
	}

	for {
		select {
		case st := <-states:
			if st.Authenticated {
				if st.Identity == nil || st.Identity.Email != "a@x.com" {
					t.Fatalf("signed-in identity = %+v", st.Identity)
				}
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("login never produced a signed-in session")
		}
	}
}

// A synchronous setup failure (loopback bind / browser launch) surfaces as a
// GraphQL error rather than a silent login:true — the whole point of running the
// flow's setup phase synchronously.
func TestLoginMutationSurfacesSetupError(t *testing.T) {
	svc := newFakeAuth(auth.Identity{Email: "a@x.com"})
	svc.loginErr = errors.New("loopback bind failed")

	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { authLoginStart }"}`))
	if !strings.Contains(raw, `"errors"`) {
		t.Fatalf("want GraphQL error for a setup failure, got: %s", raw)
	}
	if strings.Contains(raw, `"authLoginStart":true`) {
		t.Fatalf("login must not report true when setup failed, got: %s", raw)
	}
}
