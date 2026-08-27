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

package kubecatalog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// deployments and pods are the two kinds the fixtures discover.
var (
	deployments = Kind{GroupVersion: "apps/v1", Kind: "Deployment", Resource: "deployments", Namespaced: true}
	pods        = Kind{GroupVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true}
)

// probeOver is a probe whose subject connects and whose sweep answers as given. The watch
// seams are no-ops: what a run does to the standing watch is service_test.go's to assert,
// and the watcher itself watcher_test.go's.
func probeOver(f func(context.Context, *kubeconn.Connection) (sweep, error)) *catalogProbe {
	p := connectingProbe()
	p.sweep = f
	return p
}

// connectingProbe is a probe whose subject connects and whose mirror takes whatever it is
// given; a test that never sweeps uses it bare.
func connectingProbe() *catalogProbe {
	return &catalogProbe{
		conn:    func(context.Context, string) (*kubeconn.Connection, error) { return &kubeconn.Connection{}, nil },
		mirror:  func(context.Context, string, sweep, uint64) error { return nil },
		watch:   func(context.Context, string, *kubeconn.Connection) bool { return true },
		unwatch: func(string) {},
	}
}

// mirrored records what each run wrote, so a test can assert the write happened and what
// it carried.
type mirrored struct {
	writes       []sweep
	fingerprints []uint64
}

// record is the probe's mirror seam over this recorder.
func (m *mirrored) record(_ context.Context, _ string, s sweep, fingerprint uint64) error {
	m.writes = append(m.writes, s)
	m.fingerprints = append(m.fingerprints, fingerprint)
	return nil
}

// run is one pass over prev, standing in for the engine.
func run(t *testing.T, p *catalogProbe, prev *Catalog) (probe.Result, *probe.Pass[Catalog]) {
	t.Helper()
	pass := probe.NewPass("cachedcatalog/1", prev, probe.Snapshot{})
	return p.Run(context.Background(), pass), pass
}

// A subject whose context resolves to nothing parks: the outage is the cluster pass's to
// report, and the connection bridge is what brings the sweep back.
func TestRunSuspendsWithoutAConnection(t *testing.T) {
	p := connectingProbe()
	p.conn = func(context.Context, string) (*kubeconn.Connection, error) {
		return nil, kubeconn.ErrNoConnection
	}

	res, pass := run(t, p, nil)

	assert.Equal(t, probe.VerdictSuspended, res.Verdict())
	assert.Equal(t, ReasonNoConnection, res.Reason())
	_, committed := pass.Updated()
	assert.False(t, committed)
}

// A failed sweep records nothing: an empty answer is not "serves nothing", and the
// standing answer must survive for the fold to keep converging.
func TestRunFailsAndCommitsNothingWhenTheSweepFails(t *testing.T) {
	boom := errors.New("the server rejected our request")
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) { return sweep{}, boom })

	res, pass := run(t, p, &Catalog{Fingerprint: Fingerprint([]Kind{pods})})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonSweepFailed, res.Reason())
	assert.ErrorIs(t, res.Err(), boom)
	_, committed := pass.Updated()
	assert.False(t, committed)
}

// Cancellation is the caller going away, not the cluster refusing — the run records
// nothing at all rather than opening a failure streak.
func TestRunSkipsOnCancellation(t *testing.T) {
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) { return sweep{}, context.Canceled })

	res, pass := run(t, p, nil)

	assert.True(t, res.IsSkip())
	_, committed := pass.Updated()
	assert.False(t, committed)
}

// A committed value is the fold's wake, so the first answer commits — the zero Catalog
// included, which Prev cannot tell from "never swept" — an unchanged one does not, and a
// kind the server started serving does.
func TestRunCommitsOnlyOnAChange(t *testing.T) {
	served := []Kind{pods}
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) {
		return sweep{Kinds: served}, nil
	})

	res, pass := run(t, p, nil)
	require.Equal(t, probe.VerdictSucceeded, res.Verdict())
	first, committed := pass.Updated()
	require.True(t, committed, "the first answer commits")
	assert.Equal(t, Fingerprint([]Kind{pods}), first.Fingerprint)

	_, pass = run(t, p, &first)
	_, committed = pass.Updated()
	require.False(t, committed, "the same answer re-confirmed is not news")

	served = []Kind{deployments, pods}
	_, pass = run(t, p, &first)
	got, committed := pass.Updated()
	require.True(t, committed, "a kind the server started serving is news")
	assert.Equal(t, Fingerprint(served), got.Fingerprint)
}

// The watch's own health rides the standing answer, so the fold can say a cluster's discovery
// is no longer prompt. It is not a fact about what the cluster serves, so it stays out of the
// fingerprint — the word the rows on disk are matched against.
func TestRunCommitsAWatchThatWentDark(t *testing.T) {
	live := true
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) {
		return sweep{Kinds: []Kind{pods}}, nil
	})
	p.watch = func(context.Context, string, *kubeconn.Connection) bool { return live }

	_, pass := run(t, p, nil)
	first, _ := pass.Updated()
	require.True(t, first.WatchLive)

	live = false
	_, pass = run(t, p, &first)
	got, committed := pass.Updated()

	require.True(t, committed, "a watch that went dark is news")
	assert.False(t, got.WatchLive)
	assert.Equal(t, first.Fingerprint, got.Fingerprint, "the kinds did not move")
}

func TestRunCommitsAnEmptyFirstAnswer(t *testing.T) {
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) { return sweep{}, nil })

	_, pass := run(t, p, nil)

	_, committed := pass.Updated()
	assert.True(t, committed, "a cluster serving nothing mirrorable is an answer, not a wait")
}

// A partial answer is committed — a group that went quiet has not stopped being served,
// and the fold adds without pruning off it — and the run still fails, so the ladder
// retries sooner than the interval.
func TestRunCommitsAPartialAnswerAndFails(t *testing.T) {
	groupErr := errors.New("unable to retrieve the complete list of server APIs")
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) {
		return sweep{Kinds: []Kind{pods}, Partial: true}, groupErr
	})

	res, pass := run(t, p, &Catalog{Fingerprint: Fingerprint([]Kind{pods})})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonSweepPartial, res.Reason())
	got, committed := pass.Updated()
	require.True(t, committed, "the partial flag flipping is a change")
	assert.True(t, got.Partial)
}

// --- the mirror ---

// The sweep is kind_catalog's one writer, so every answer it produces goes to disk —
// whether or not it moved. That is what puts a table wiped under the sweep back, with no
// repair protocol to carry the fact that it was.
func TestRunWritesEveryAnswerWhetherOrNotItMoved(t *testing.T) {
	var m mirrored
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) { return sweep{Kinds: []Kind{pods}}, nil })
	p.mirror = m.record

	_, pass := run(t, p, nil)
	first, committed := pass.Updated()
	require.True(t, committed)

	_, pass = run(t, p, &first)
	_, committed = pass.Updated()
	require.False(t, committed, "the same answer re-confirmed is not news")

	assert.Equal(t, []sweep{{Kinds: []Kind{pods}}, {Kinds: []Kind{pods}}}, m.writes,
		"the second run did not rewrite the rows")
}

// The fingerprint stored beside the rows is the one the observable carries, which is the
// whole of what lets the fold tell a table this sweep wrote from one wiped under it. Two
// hashes taken at different moments would answer that question about different answers.
func TestRunWritesTheFingerprintItCommits(t *testing.T) {
	var m mirrored
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) {
		return sweep{Kinds: []Kind{pods}}, nil
	})
	p.mirror = m.record

	_, pass := run(t, p, nil)

	committed, ok := pass.Updated()
	require.True(t, ok)
	assert.Equal(t, []uint64{committed.Fingerprint}, m.fingerprints)
}

// The write comes first, because a commit is the fold's wake: one over rows that are not
// there would have the fold converge on a table the write is still catching up to.
func TestRunCommitsNothingWhenTheWriteFails(t *testing.T) {
	full := errors.New("database or disk is full")
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) { return sweep{Kinds: []Kind{pods}}, nil })
	p.mirror = func(context.Context, string, sweep, uint64) error { return full }

	res, pass := run(t, p, nil)

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonStoreFailed, res.Reason())
	assert.ErrorIs(t, res.Err(), full)
	_, committed := pass.Updated()
	assert.False(t, committed)
}

// A failed write outranks a partial answer: nothing was committed at all, and reporting
// the incomplete answer would point a reader at an api group that is not the problem.
func TestAFailedWriteOutranksAPartialAnswer(t *testing.T) {
	full := errors.New("database or disk is full")
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) {
		return sweep{Kinds: []Kind{pods}, Partial: true}, errors.New("unable to retrieve the complete list of server APIs")
	})
	p.mirror = func(context.Context, string, sweep, uint64) error { return full }

	res, pass := run(t, p, nil)

	assert.Equal(t, ReasonStoreFailed, res.Reason())
	_, committed := pass.Updated()
	assert.False(t, committed)
}

// A removed store is a cache being torn down, with this subject's Forget on its way:
// there is nothing to write into and nothing worth reporting, so the run parks rather
// than opening a failure streak against a record that is going.
func TestRunSuspendsWhenTheStoreIsRemoved(t *testing.T) {
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) { return sweep{Kinds: []Kind{pods}}, nil })
	p.mirror = func(context.Context, string, sweep, uint64) error {
		return fmt.Errorf("open cache 1: %w", kubestore.ErrRemoved)
	}

	res, pass := run(t, p, nil)

	assert.Equal(t, probe.VerdictSuspended, res.Verdict())
	assert.Equal(t, ReasonStoreRemoved, res.Reason())
	_, committed := pass.Updated()
	assert.False(t, committed)
}

// A failed sweep writes nothing: an empty answer is not "serves nothing", and rows laid
// down off one would prune a catalog the run never read.
func TestAFailedSweepWritesNothing(t *testing.T) {
	var m mirrored
	p := probeOver(func(context.Context, *kubeconn.Connection) (sweep, error) {
		return sweep{}, errors.New("the server rejected our request")
	})
	p.mirror = m.record

	run(t, p, &Catalog{Fingerprint: Fingerprint([]Kind{pods})})

	assert.Empty(t, m.writes)
}

// --- discovery filtering ---

// A cache mirrors collections it can list and watch. A subresource has none of its own,
// and the alternate events spelling is the same store served twice — syncing it would
// cache every event a second time.
func TestServedKindsFiltersWhatCannotBeMirrored(t *testing.T) {
	lists := []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: []string{"list", "watch", "get"}},
			{Name: "pods/log", Kind: "Pod", Namespaced: true, Verbs: []string{"get"}},
			{Name: "bindings", Kind: "Binding", Namespaced: true, Verbs: []string{"create"}},
			{Name: "events", Kind: "Event", Namespaced: true, Verbs: []string{"list", "watch"}},
		}},
		{GroupVersion: "events.k8s.io/v1", APIResources: []metav1.APIResource{
			{Name: "events", Kind: "Event", Namespaced: true, Verbs: []string{"list", "watch"}},
		}},
		nil,
	}

	got := servedKinds(lists)

	assert.Equal(t, []Kind{
		{GroupVersion: "v1", Kind: "Event", Resource: "events", Namespaced: true},
		{GroupVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true},
	}, got, "sorted, so a sweep is deterministic")
}

// --- CRDs ---

// fakeDiscovery answers ServerPreferredResources and nothing else; the embedded interface
// is nil, so a call this package does not make panics rather than passing silently.
type fakeDiscovery struct {
	discovery.DiscoveryInterface
	lists []*metav1.APIResourceList
}

func (f fakeDiscovery) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return f.lists, nil
}

// crd is one CustomResourceDefinition body, carrying only what the match reads.
func crd(group, plural string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": plural + "." + group},
		"spec": map[string]any{
			"group": group,
			"names": map[string]any{"plural": plural},
		},
	}}
}

// servingDynamic is a cluster serving these CustomResourceDefinitions.
func servingDynamic(t *testing.T, crds ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"})
	client.PrependReactor("list", crdGVR.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		list := &unstructured.UnstructuredList{}
		list.SetAPIVersion("apiextensions.k8s.io/v1")
		list.SetKind("CustomResourceDefinitionList")
		for _, c := range crds {
			list.Items = append(list.Items, *c)
		}
		return true, list, nil
	})
	return client
}

// refusingDynamic is a cluster whose credentials cannot list CustomResourceDefinitions.
func refusingDynamic(t *testing.T) *dynamicfake.FakeDynamicClient {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"})
	client.PrependReactor("list", crdGVR.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(crdGVR.GroupResource(), "", errors.New("nope"))
	})
	return client
}

// One CRD serves several versions, so the match is on group and plural: keying on the
// version would leave every kind discovered at another one reading as built-in.
func TestMarkCRDsMatchesOnGroupAndPlural(t *testing.T) {
	kinds := []Kind{
		{GroupVersion: "v1", Kind: "Pod", Resource: "pods"},
		{GroupVersion: "example.com/v2", Kind: "Widget", Resource: "widgets"},
	}

	markCRDs(kinds, []crdRef{{group: "example.com", plural: "widgets"}})

	assert.Equal(t, []Kind{
		{GroupVersion: "v1", Kind: "Pod", Resource: "pods"},
		{GroupVersion: "example.com/v2", Kind: "Widget", Resource: "widgets", IsCRD: true},
	}, kinds, "the CRD was discovered at a version its definition does not name")
}

// The whole sweep, with a cluster serving one custom resource and one built-in.
func TestDiscoverServedKindsMarksTheCustomResources(t *testing.T) {
	conn := &kubeconn.Connection{
		Discovery: fakeDiscovery{lists: []*metav1.APIResourceList{
			{GroupVersion: "v1", APIResources: []metav1.APIResource{
				{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: []string{"list", "watch"}},
			}},
			{GroupVersion: "example.com/v1", APIResources: []metav1.APIResource{
				{Name: "widgets", Kind: "Widget", Namespaced: true, Verbs: []string{"list", "watch"}},
			}},
		}},
		Dynamic: servingDynamic(t, crd("example.com", "widgets")),
	}

	got, err := discoverServedKinds(context.Background(), conn)

	require.NoError(t, err)
	assert.Equal(t, []Kind{
		{GroupVersion: "example.com/v1", Kind: "Widget", Resource: "widgets", Namespaced: true, IsCRD: true},
		{GroupVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true},
	}, got.Kinds)
}

// Listing CRDs is a cluster-scoped read RBAC commonly denies, and discovery is useful
// without it: a refusal leaves every kind reading as built-in rather than taking the
// catalog away from a user the sweep otherwise serves.
func TestARefusedCRDListIsNotAFailedSweep(t *testing.T) {
	conn := &kubeconn.Connection{
		Discovery: fakeDiscovery{lists: []*metav1.APIResourceList{{
			GroupVersion: "example.com/v1",
			APIResources: []metav1.APIResource{
				{Name: "widgets", Kind: "Widget", Namespaced: true, Verbs: []string{"list", "watch"}},
			},
		}}},
		Dynamic: refusingDynamic(t),
	}

	got, err := discoverServedKinds(context.Background(), conn)

	require.NoError(t, err)
	assert.False(t, got.Partial, "a refused CRD list made the answer partial")
	require.Len(t, got.Kinds, 1)
	assert.False(t, got.Kinds[0].IsCRD)
}

// The bit is part of the answer, so a cluster whose CRD list starts answering is news
// even though the kinds themselves have not moved.
func TestKindsFingerprintCoversTheCRDBit(t *testing.T) {
	built := Kind{GroupVersion: "example.com/v1", Kind: "Widget", Resource: "widgets"}
	custom := built
	custom.IsCRD = true

	assert.NotEqual(t, Fingerprint([]Kind{built}), Fingerprint([]Kind{custom}))
}
