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

package kubesync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// dynamicSource is a thin adapter over client-go, so these tests run it against an api
// server stand-in rather than a fake client: what needs pinning is the REQUEST it builds
// (which the fake clients synthesize away) and what it makes of a real response body.

var podsGVR = schema.GroupVersionResource{Version: "v1", Resource: "pods"}

type recordedRequest struct {
	Method string
	Path   string
	Query  url.Values
}

// requestLog captures what the source asked the server for.
type requestLog struct {
	mu  sync.Mutex
	all []recordedRequest
}

func (l *requestLog) record(r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.all = append(l.all, recordedRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()})
}

// last returns the most recent request, failing the test if none was made.
func (l *requestLog) last(t *testing.T) recordedRequest {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	require.NotEmpty(t, l.all, "the source issued no request")
	return l.all[len(l.all)-1]
}

// newTestSource starts an api-server stand-in answering every request with h, and returns
// a production Source pointed at it.
func newTestSource(t *testing.T, gvr schema.GroupVersionResource, h http.HandlerFunc) (Source, *requestLog) {
	t.Helper()
	log := &requestLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		h(w, r)
	}))
	t.Cleanup(srv.Close)

	src, err := NewDynamicSource(&rest.Config{Host: srv.URL}, gvr)
	require.NoError(t, err)
	return src, log
}

// respondJSON writes body as an API response.
func respondJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

// A LIST forwards the caller's pagination options and hands back the three things the
// driver's state machine runs on: the bodies, the continue token, and the list's
// resourceVersion.
func TestDynamicSourceListReturnsItemsContinueAndResourceVersion(t *testing.T) {
	src, log := newTestSource(t, podsGVR, func(w http.ResponseWriter, _ *http.Request) {
		respondJSON(w, `{
			"apiVersion": "v1",
			"kind": "PodList",
			"metadata": {"resourceVersion": "42", "continue": "next-page"},
			"items": [
				{"apiVersion":"v1","kind":"Pod","metadata":{"uid":"uid-a","name":"a","namespace":"default","resourceVersion":"7"}}
			]
		}`)
	})

	items, cont, rv, err := src.List(context.Background(), metav1.ListOptions{Limit: 500, Continue: "tok"})
	require.NoError(t, err)

	require.Len(t, items, 1)
	assert.Equal(t, "uid-a", string(items[0].GetUID()))
	assert.Equal(t, "next-page", cont)
	assert.Equal(t, "42", rv)

	// Core-group kinds live under /api, and the list is cluster-wide — one worker mirrors
	// every namespace.
	got := log.last(t)
	assert.Equal(t, http.MethodGet, got.Method)
	assert.Equal(t, "/api/v1/pods", got.Path)
	assert.Equal(t, "500", got.Query.Get("limit"))
	assert.Equal(t, "tok", got.Query.Get("continue"))
}

// A LIST failure surfaces as an error with no partial answer — the driver treats a pass
// that yields no usable resourceVersion as a failure and backs off.
func TestDynamicSourceListReportsServerErrors(t *testing.T) {
	src, _ := newTestSource(t, podsGVR, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		respondJSON(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","code":403}`)
	})

	items, cont, rv, err := src.List(context.Background(), metav1.ListOptions{})
	require.Error(t, err)
	assert.Nil(t, items)
	assert.Empty(t, cont)
	assert.Empty(t, rv)
}

// The metadata list is the diff resync's cheap read: it must project exactly the four
// fields the diff compares and addresses by, and pass the pagination cursor through.
func TestDynamicSourceListMetadataProjectsIdentities(t *testing.T) {
	src, log := newTestSource(t, podsGVR, func(w http.ResponseWriter, _ *http.Request) {
		respondJSON(w, `{
			"apiVersion": "meta.k8s.io/v1",
			"kind": "PartialObjectMetadataList",
			"metadata": {"resourceVersion": "99", "continue": "more"},
			"items": [
				{"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadata","metadata":{"uid":"uid-a","name":"a","namespace":"default","resourceVersion":"7"}},
				{"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadata","metadata":{"uid":"uid-b","name":"b","namespace":"kube-system","resourceVersion":"8"}}
			]
		}`)
	})

	metas, cont, rv, err := src.ListMetadata(context.Background(), metav1.ListOptions{Limit: 500})
	require.NoError(t, err)

	assert.Equal(t, []ObjectMeta{
		{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "7"},
		{UID: "uid-b", Namespace: "kube-system", Name: "b", ResourceVersion: "8"},
	}, metas)
	assert.Equal(t, "more", cont)
	assert.Equal(t, "99", rv)

	assert.Equal(t, "/api/v1/pods", log.last(t).Path)
}

// Not every api server serves a metadata endpoint. The error has to come back rather than
// be swallowed, since it is what makes the driver fall back to a full LIST.
func TestDynamicSourceListMetadataReportsServerErrors(t *testing.T) {
	src, _ := newTestSource(t, podsGVR, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		respondJSON(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404}`)
	})

	metas, _, _, err := src.ListMetadata(context.Background(), metav1.ListOptions{})
	require.Error(t, err)
	assert.Nil(t, metas)
}

// Get takes the namespace as a plain argument with no cluster-scoped branch, which only
// works because the dynamic client omits the segment entirely for an empty one. If that
// ever stopped holding, every cluster-scoped kind's diff resync would address the wrong
// path.
func TestDynamicSourceGetOmitsTheNamespaceSegmentWhenEmpty(t *testing.T) {
	body := `{"apiVersion":"v1","kind":"Pod","metadata":{"uid":"uid-a","name":"a","resourceVersion":"7"}}`

	tests := []struct {
		name      string
		namespace string
		wantPath  string
	}{
		{"namespaced", "default", "/api/v1/namespaces/default/pods/a"},
		{"cluster-scoped", "", "/api/v1/pods/a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, log := newTestSource(t, podsGVR, func(w http.ResponseWriter, _ *http.Request) {
				respondJSON(w, body)
			})

			u, err := src.Get(context.Background(), tt.namespace, "a")
			require.NoError(t, err)
			assert.Equal(t, "uid-a", string(u.GetUID()))
			assert.Equal(t, tt.wantPath, log.last(t).Path)
		})
	}
}

// A non-core group is served under /apis/<group>/<version> rather than /api/<version>.
func TestDynamicSourceAddressesNonCoreGroups(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	src, log := newTestSource(t, gvr, func(w http.ResponseWriter, _ *http.Request) {
		respondJSON(w, `{"apiVersion":"apps/v1","kind":"DeploymentList","metadata":{"resourceVersion":"1"},"items":[]}`)
	})

	_, _, _, err := src.List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Equal(t, "/apis/apps/v1/deployments", log.last(t).Path)
}

// Watch hands the caller's options — RetryWatcher supplies the resourceVersion and the
// bookmark opt-in — to the server and returns the live event stream unwrapped.
func TestDynamicSourceWatchStreamsEvents(t *testing.T) {
	src, log := newTestSource(t, podsGVR, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"type":"ADDED","object":{"apiVersion":"v1","kind":"Pod","metadata":{"uid":"uid-a","name":"a","resourceVersion":"7"}}}` + "\n"))
		w.(http.Flusher).Flush()
	})

	wch, err := src.Watch(context.Background(), metav1.ListOptions{ResourceVersion: "5", AllowWatchBookmarks: true})
	require.NoError(t, err)
	t.Cleanup(wch.Stop)

	ev := testutil.Recv(t, wch.ResultChan(), "the watch event")
	assert.Equal(t, watch.Added, ev.Type)

	got := log.last(t)
	assert.Equal(t, "/api/v1/pods", got.Path)
	assert.Equal(t, "true", got.Query.Get("watch"))
	assert.Equal(t, "5", got.Query.Get("resourceVersion"))
	assert.Equal(t, "true", got.Query.Get("allowWatchBookmarks"))
}

// A watch the server refuses fails at open, which is the signal the driver charges against
// its error budget rather than a stream that opens and carries nothing.
func TestDynamicSourceWatchReportsServerErrors(t *testing.T) {
	src, _ := newTestSource(t, podsGVR, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		respondJSON(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","code":403}`)
	})

	_, err := src.Watch(context.Background(), metav1.ListOptions{})
	require.Error(t, err)
}

// A config the client can't build from is rejected at construction, so a misconfigured
// worker never starts rather than failing on its first pass.
func TestNewDynamicSourceRejectsAnUnusableConfig(t *testing.T) {
	_, err := NewDynamicSource(&rest.Config{Host: "://not-a-url"}, podsGVR)
	assert.Error(t, err)
}
