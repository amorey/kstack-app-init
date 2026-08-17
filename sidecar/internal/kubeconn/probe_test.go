// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubeconn

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

// apiServer serves the three probe paths from handlers keyed by path, so a test names
// only the responses it cares about; anything unnamed 404s like a real server.
func apiServer(t *testing.T, routes map[string]http.HandlerFunc) *Connection {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, ok := routes[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		h(w, r)
	}))
	t.Cleanup(srv.Close)

	svc := newTestService()
	t.Cleanup(func() { _ = svc.Close() })
	e, err := svc.entryFor(&rest.Config{Host: srv.URL}, "probe")
	require.NoError(t, err)
	return e.conn
}

func jsonBody(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

var okRoutes = map[string]http.HandlerFunc{
	"/version":                       jsonBody(`{"gitVersion":"v1.31.2"}`),
	"/api/v1/namespaces/kube-system": jsonBody(`{"metadata":{"uid":"cluster-uid"}}`),
	"/apis/authentication.k8s.io/v1/selfsubjectreviews": jsonBody(`{"status":{"userInfo":{"username":"alice"}}}`),
}

func TestProbeReadsTheClusterIdentity(t *testing.T) {
	id, err := Probe(t.Context(), apiServer(t, okRoutes))
	require.NoError(t, err)

	require.Equal(t, "cluster-uid", id.ServerUID)
	require.Equal(t, "v1.31.2", id.ServerVersion)
	require.Equal(t, "alice", id.Username)
	require.NoError(t, id.UIDErr)
}

// A namespace-scoped user is common on shared clusters. The connection is good, so the
// probe succeeds and says which field it could not fill.
func TestProbeSurvivesAForbiddenNamespaceRead(t *testing.T) {
	routes := withRoute(okRoutes, "/api/v1/namespaces/kube-system", status(http.StatusForbidden))

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.NoError(t, err)
	require.Empty(t, id.ServerUID)
	require.ErrorContains(t, id.UIDErr, "403")
	require.Equal(t, "v1.31.2", id.ServerVersion, "the other requests must not be affected")
	require.Equal(t, "alice", id.Username)
}

// A create endpoint decodes a resource from the body, so an empty POST is a 400 and the
// username comes back missing against every real server — which a handler that ignores
// the request body would never catch.
func TestProbePostsASelfSubjectReview(t *testing.T) {
	var got struct {
		APIVersion  string `json:"apiVersion"`
		Kind        string `json:"kind"`
		ContentType string
	}
	routes := withRoute(okRoutes, reviewPath, func(w http.ResponseWriter, r *http.Request) {
		got.ContentType = r.Header.Get("Content-Type")
		// assert, not require: this runs on the server's goroutine, where FailNow would
		// abandon the handler rather than fail the test. The assertions below carry it.
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		jsonBody(`{"status":{"userInfo":{"username":"alice"}}}`)(w, r)
	})

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.NoError(t, err)

	assert.Equal(t, "authentication.k8s.io/v1", got.APIVersion)
	assert.Equal(t, "SelfSubjectReview", got.Kind)
	assert.Equal(t, "application/json", got.ContentType)
	assert.Equal(t, "alice", id.Username)
}

// A server that rejects the request still leaves the rest of the probe intact.
func TestProbeSurvivesARejectedReview(t *testing.T) {
	routes := withRoute(okRoutes, reviewPath, status(http.StatusBadRequest))

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.NoError(t, err)
	require.Empty(t, id.Username)
	require.Equal(t, "cluster-uid", id.ServerUID)
}

// selfsubjectreviews is authentication.k8s.io/v1 only from 1.28.
func TestProbeSurvivesAnOlderServer(t *testing.T) {
	routes := withRoute(okRoutes, "/apis/authentication.k8s.io/v1/selfsubjectreviews", status(http.StatusNotFound))

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.NoError(t, err)
	require.Empty(t, id.Username)
	require.Equal(t, "cluster-uid", id.ServerUID)
}

func TestProbeSurvivesAnUnreadableVersion(t *testing.T) {
	routes := withRoute(okRoutes, "/version", jsonBody(`not json`))

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.NoError(t, err)
	require.Empty(t, id.ServerVersion)
	require.Equal(t, "cluster-uid", id.ServerUID)
}

func TestProbeReportsAnUnreadableIdentity(t *testing.T) {
	routes := withRoute(okRoutes, "/api/v1/namespaces/kube-system", jsonBody(`not json`))

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.NoError(t, err)
	require.Empty(t, id.ServerUID)
	require.Error(t, id.UIDErr)
}

func TestProbeFailsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := Probe(ctx, apiServer(t, okRoutes))
	require.ErrorIs(t, err, context.Canceled)
}

// Concurrent, so the probe costs one round trip rather than three. Each handler parks
// until all three have arrived, which only completes if they overlap.
func TestProbeIssuesItsRequestsConcurrently(t *testing.T) {
	var arrived atomic.Int32
	all := make(chan struct{})
	gate := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if arrived.Add(1) == 3 {
				close(all)
			}
			select {
			case <-all:
			case <-r.Context().Done():
			}
			jsonBody(body)(w, r)
		}
	}

	conn := apiServer(t, map[string]http.HandlerFunc{
		"/version":                       gate(`{"gitVersion":"v1.31.2"}`),
		"/api/v1/namespaces/kube-system": gate(`{"metadata":{"uid":"cluster-uid"}}`),
		"/apis/authentication.k8s.io/v1/selfsubjectreviews": gate(`{"status":{"userInfo":{"username":"alice"}}}`),
	})

	id, err := Probe(t.Context(), conn)
	require.NoError(t, err)
	require.Equal(t, "cluster-uid", id.ServerUID)
}

// withRoute returns a copy of routes with one path replaced.
func withRoute(routes map[string]http.HandlerFunc, path string, h http.HandlerFunc) map[string]http.HandlerFunc {
	out := make(map[string]http.HandlerFunc, len(routes))
	for k, v := range routes {
		out[k] = v
	}
	out[path] = h
	return out
}

// A Connection whose base URL cannot be joined into a request URL: the failure is the
// request build itself, ahead of any transport.
func TestProbeReportsAnUnbuildableRequest(t *testing.T) {
	_, err := Probe(t.Context(), &Connection{
		BaseURL:    &url.URL{Scheme: "ht tp", Host: "one.example"},
		HTTPClient: http.DefaultClient,
	})
	require.Error(t, err)
}

// Only a transport failure fails the probe: it is the one outcome that says nothing
// reached the API server. The error names the path and keeps the cause, which is what a
// caller logs.
func TestProbeFailsWhenNothingAnswers(t *testing.T) {
	svc := newTestService()
	defer svc.Close()
	e, err := svc.entryFor(&rest.Config{Host: "http://127.0.0.1:1"}, "dead")
	require.NoError(t, err)

	_, err = Probe(t.Context(), e.conn)
	require.ErrorIs(t, err, errTransport)
	assert.ErrorContains(t, err, "connection refused")
}

// The field exists so a caller can tell 403 from 404, which comparing by presence alone
// would throw away — and a probe that reached the server carries a status line, not the
// ephemeral text a transport failure has.
func TestIdentitySameAsDistinguishesUIDFailures(t *testing.T) {
	forbidden := Identity{UIDErr: errors.New("/api/v1/namespaces/kube-system: 403 Forbidden")}
	missing := Identity{UIDErr: errors.New("/api/v1/namespaces/kube-system: 404 Not Found")}

	assert.False(t, forbidden.sameAs(missing), "a different refusal is different news")
	assert.True(t, forbidden.sameAs(Identity{UIDErr: errors.New(forbidden.UIDErr.Error())}))
	assert.False(t, forbidden.sameAs(Identity{}), "gaining the UID is news")
	assert.True(t, Identity{}.sameAs(Identity{}))
}

// Expired or revoked credentials 401 on every path. Reporting that as a successful probe
// with an empty identity would be indistinguishable from a healthy cluster whose contents
// this user may not read — and would hand out a connection nothing can be done with.
func TestProbeFailsWhenEveryRequestIsRefused(t *testing.T) {
	routes := map[string]http.HandlerFunc{}
	for path := range okRoutes {
		routes[path] = status(http.StatusUnauthorized)
	}

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Equal(t, Identity{}, id, "nothing was learned, so nothing is reported")
}

// One refusal among three is the case the design protects: the connection works, this user
// just may not read that path.
func TestProbeSurvivesAPartialRefusal(t *testing.T) {
	routes := withRoute(okRoutes, "/version", status(http.StatusUnauthorized))

	id, err := Probe(t.Context(), apiServer(t, routes))
	require.NoError(t, err)
	assert.Empty(t, id.ServerVersion)
	assert.Equal(t, "cluster-uid", id.ServerUID)
	assert.Equal(t, "alice", id.Username)
}
