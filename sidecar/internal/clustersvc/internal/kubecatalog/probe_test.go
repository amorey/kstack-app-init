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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
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
func probeOver(sweep func(*kubeconn.Connection) (Catalog, error)) *catalogProbe {
	p := connectingProbe()
	p.sweep = sweep
	return p
}

// connectingProbe is a probe whose subject connects; a test that never sweeps uses it bare.
func connectingProbe() *catalogProbe {
	return &catalogProbe{
		conn:    func(context.Context, string) (*kubeconn.Connection, error) { return &kubeconn.Connection{}, nil },
		watch:   func(context.Context, string, *kubeconn.Connection) {},
		unwatch: func(string) {},
	}
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
	p := probeOver(func(*kubeconn.Connection) (Catalog, error) { return Catalog{}, boom })

	res, pass := run(t, p, &Catalog{Kinds: []Kind{pods}})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonSweepFailed, res.Reason())
	assert.ErrorIs(t, res.Err(), boom)
	_, committed := pass.Updated()
	assert.False(t, committed)
}

// Cancellation is the caller going away, not the cluster refusing — the run records
// nothing at all rather than opening a failure streak.
func TestRunSkipsOnCancellation(t *testing.T) {
	p := probeOver(func(*kubeconn.Connection) (Catalog, error) { return Catalog{}, context.Canceled })

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
	p := probeOver(func(*kubeconn.Connection) (Catalog, error) {
		return Catalog{Kinds: served}, nil
	})

	res, pass := run(t, p, nil)
	require.Equal(t, probe.VerdictSucceeded, res.Verdict())
	first, committed := pass.Updated()
	require.True(t, committed, "the first answer commits")
	assert.Equal(t, []Kind{pods}, first.Kinds)

	_, pass = run(t, p, &first)
	_, committed = pass.Updated()
	require.False(t, committed, "the same answer re-confirmed is not news")

	served = []Kind{deployments, pods}
	_, pass = run(t, p, &first)
	got, committed := pass.Updated()
	require.True(t, committed, "a kind the server started serving is news")
	assert.Equal(t, served, got.Kinds)
}

func TestRunCommitsAnEmptyFirstAnswer(t *testing.T) {
	p := probeOver(func(*kubeconn.Connection) (Catalog, error) { return Catalog{}, nil })

	_, pass := run(t, p, nil)

	_, committed := pass.Updated()
	assert.True(t, committed, "a cluster serving nothing mirrorable is an answer, not a wait")
}

// A partial answer is committed — a group that went quiet has not stopped being served,
// and the fold adds without pruning off it — and the run still fails, so the ladder
// retries sooner than the interval.
func TestRunCommitsAPartialAnswerAndFails(t *testing.T) {
	groupErr := errors.New("unable to retrieve the complete list of server APIs")
	p := probeOver(func(*kubeconn.Connection) (Catalog, error) {
		return Catalog{Kinds: []Kind{pods}, Partial: true}, groupErr
	})

	res, pass := run(t, p, &Catalog{Kinds: []Kind{pods}})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonSweepPartial, res.Reason())
	got, committed := pass.Updated()
	require.True(t, committed, "the partial flag flipping is a change")
	assert.True(t, got.Partial)
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
