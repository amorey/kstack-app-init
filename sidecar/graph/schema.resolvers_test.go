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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/graph"
	"github.com/kubetail-org/kstack-app/sidecar/internal/auth"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
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

// clustersWatch degrades to a closed stream when no cluster cache is configured
// (a bare Resolver has no ClusterManager), per the nil-tolerant resolver
// contract: gqlgen flushes a terminal `complete` with no `next` snapshot rather
// than erroring. (The per-cluster sync-status surface clusterSyncStatusWatch was
// removed; status is now derived from the present/enabled/cached flags on
// clustersWatch.)
func TestClustersWatchDegradesWithoutCache(t *testing.T) {
	h := graph.NewServer(&graph.Resolver{})
	ts := httptest.NewServer(h)
	defer ts.Close()

	resp := openSSESubscription(t, ts.URL, "",
		"subscription { clustersWatch { uuid name present enabled cached } }")
	defer resp.Body.Close() // ends the subscription; must run before ts.Close()
	events := sseEvents(resp)

	if ev := nextSSE(t, events); ev.event != "complete" {
		t.Fatalf("want event=complete (degraded stream), got %q (data=%s)", ev.event, ev.data)
	}
}

// --- kubeConfig / setCurrentContext ---------------------------------------

// writeKubeConfig writes a kubeconfig with the named contexts and the given
// current-context, returning the path.
func writeKubeConfig(t *testing.T, current string, contexts ...string) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	for _, name := range contexts {
		cfg.Clusters["cluster-"+name] = &clientcmdapi.Cluster{}
		cfg.AuthInfos["user-"+name] = &clientcmdapi.AuthInfo{}
		cfg.Contexts[name] = &clientcmdapi.Context{Cluster: "cluster-" + name, AuthInfo: "user-" + name}
	}
	cfg.CurrentContext = current
	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, clientcmd.WriteToFile(*cfg, path))
	return path
}

// newKubeConfigStack stands up a sidecar GraphQL server backed by a real
// KubeConfigWatcher over the given kubeconfig path.
func newKubeConfigStack(t *testing.T, kubeconfigPath string) (url string, watcher *k8shelpers.KubeConfigWatcher) {
	t.Helper()
	w, err := k8shelpers.NewKubeConfigWatcher(kubeconfigPath)
	require.NoError(t, err)
	t.Cleanup(w.Close)

	r := &graph.Resolver{KubeConfigWatcher: w}
	srv := httptest.NewServer(graph.NewServer(r))
	t.Cleanup(srv.Close)
	return srv.URL, w
}

func TestSetCurrentContext_Mutation_Success(t *testing.T) {
	path := writeKubeConfig(t, "context-A", "context-A", "context-B")
	url, w := newKubeConfigStack(t, path)

	raw := postGQL(t, url, `{"query":"mutation { setCurrentContext(name: \"context-B\") }"}`)
	if !strings.Contains(string(raw), `"setCurrentContext":true`) {
		t.Fatalf("response: %s", raw)
	}

	assert.Equal(t, "context-B", w.Get().CurrentContext)

	onDisk, err := clientcmd.LoadFromFile(path)
	require.NoError(t, err)
	assert.Equal(t, "context-B", onDisk.CurrentContext)
}

func TestSetCurrentContext_Mutation_UnknownContext(t *testing.T) {
	path := writeKubeConfig(t, "context-A", "context-A", "context-B")
	url, w := newKubeConfigStack(t, path)

	raw := postGQL(t, url, `{"query":"mutation { setCurrentContext(name: \"nope\") }"}`)
	if !strings.Contains(string(raw), `"errors"`) {
		t.Fatalf("expected GraphQL error, got: %s", raw)
	}
	assert.Equal(t, "context-A", w.Get().CurrentContext)
}

// A handler with no watcher (Config{}) must return a clean GraphQL error
// rather than panicking — mirrors the KubeConfigWatch nil-guard.
func TestSetCurrentContext_Mutation_NilWatcher(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{}))
	t.Cleanup(srv.Close)

	raw := postGQL(t, srv.URL, `{"query":"mutation { setCurrentContext(name: \"x\") }"}`)
	if !strings.Contains(string(raw), `"errors"`) {
		t.Fatalf("expected GraphQL error, got: %s", raw)
	}
}

// --- cloud account: authState / login / logout / authStateWatch -----------
//
// The fakes (memCredStore/fakeOAuthFlow/fakeLoopback) live in testutils_test.go;
// the postGQL / SSE helpers are defined above + in server_test.go.

// The authState query degrades to signed-out on a bare Resolver (no cloud account).
func TestAuthStateQueryDegradesSignedOut(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{}))
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
	events := sseEvents(resp)

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

// authStateWatch degrades to a closed stream (terminal complete) without a cloud
// account, per the nil-tolerant resolver contract.
func TestAuthStateWatchDegradesWithoutAccount(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{}))
	defer srv.Close()

	resp := openSSESubscription(t, srv.URL, "", "subscription { authStateWatch { identity { sub } } }")
	defer resp.Body.Close()
	events := sseEvents(resp)

	if ev := nextSSE(t, events); ev.event != "complete" {
		t.Fatalf("want complete (degraded stream), got %q (data=%s)", ev.event, ev.data)
	}
}

// logout delegates to the account: returns true and flips the session signed-out.
func TestLogoutMutation(t *testing.T) {
	svc := signedInFakeAuth(auth.Identity{})
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{Auth: svc}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { logout }"}`))
	if !strings.Contains(raw, `"logout":true`) {
		t.Fatalf("want logout true, got: %s", raw)
	}
	if cur, _ := svc.Current(context.Background()); cur.Authenticated {
		t.Fatal("session still signed in after logout")
	}
}

// logout errors cleanly (no panic) when no cloud account is configured.
func TestLogoutMutationDegradesError(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { logout }"}`))
	if !strings.Contains(raw, `"errors"`) {
		t.Fatalf("want GraphQL error, got: %s", raw)
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

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { startLogin }"}`))
	if !strings.Contains(raw, `"startLogin":true`) {
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

// login errors cleanly when no cloud account is configured.
func TestLoginMutationDegradesError(t *testing.T) {
	srv := httptest.NewServer(graph.NewServer(&graph.Resolver{}))
	defer srv.Close()

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { startLogin }"}`))
	if !strings.Contains(raw, `"errors"`) {
		t.Fatalf("want GraphQL error, got: %s", raw)
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

	raw := string(postGQL(t, srv.URL, `{"query":"mutation { startLogin }"}`))
	if !strings.Contains(raw, `"errors"`) {
		t.Fatalf("want GraphQL error for a setup failure, got: %s", raw)
	}
	if strings.Contains(raw, `"startLogin":true`) {
		t.Fatalf("login must not report true when setup failed, got: %s", raw)
	}
}
