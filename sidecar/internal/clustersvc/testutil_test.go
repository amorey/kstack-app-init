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

// Fixtures shared across this package's test files. Anything used by one file lives
// beside it instead.
package clustersvc

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/amorey/beehive"
	beehivesqlite "github.com/amorey/beehive/sqlite"
	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// newTestBeehive returns a beehive over an in-memory store, closed on cleanup. The
// bootstrap New does, minus the disk. opts let a test shrink a cadence it would
// otherwise have to outwait.
func newTestBeehive(t *testing.T, opts ...beehive.Option) *beehive.Beehive {
	t.Helper()
	store, err := beehivesqlite.OpenMemory()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	bh, err := beehive.New(store, opts...)
	require.NoError(t, err)
	return bh
}

// newTestDeps returns the shared set over a test beehive — the same newDeps the
// composition root calls, so a test never assembles its own. The controllers are
// registered so a requeue reaches a real reconciler, but beehive is never run, so
// nothing reconciles or collects behind these tests.
//
// One store for every kind, which the owner edges need: beehive refuses an owner in
// another store.
func newTestDeps(t *testing.T) deps {
	t.Helper()
	d, _ := newTestDepsAndBeehive(t)
	return d
}

// fakeKubeconn stands in for the pool: it answers per context from a map and records who
// was asked for. The zero value refuses nothing and knows nothing, which is a fleet whose
// first probe is still owed — what a cluster pass finds unless a test says otherwise.
type fakeKubeconn struct {
	states map[string]kubeconn.State
	// hub is the fleet feed, keyed by context the way the pool keys it. Built on demand so
	// the zero value stays usable.
	once  sync.Once
	hub   *conflate.Hub[string, struct{}]
	asked []string

	released []string
}

func (f *fakeKubeconn) Acquire(contextName string) kubeconn.Lease {
	f.asked = append(f.asked, contextName)
	return &fakeLease{svc: f, contextName: contextName, state: f.states[contextName]}
}

// Subscribe is the fleet feed the trigger reads. publish is the probe landing on it.
func (f *fakeKubeconn) Subscribe() kubeconn.Subscription { return f.moved().Receiver() }

func (f *fakeKubeconn) publish(contextName string) {
	f.moved().Sender().Send(contextName, struct{}{})
}

func (f *fakeKubeconn) moved() *conflate.Hub[string, struct{}] {
	f.once.Do(func() { f.hub = conflate.New[string, struct{}]() })
	return f.hub
}

// fakeLease is one claim, holding what a probe would have found. It records its release so
// a test can pin the lifetime a pass is meant to keep.
type fakeLease struct {
	svc         *fakeKubeconn
	contextName string
	state       kubeconn.State
	// departed is what the pool would report for a context the kubeconfig stopped naming.
	departed bool
}

func (l *fakeLease) Conn(context.Context) (*kubeconn.Connection, error) {
	panic("a cluster pass claims, it does not dial")
}

func (l *fakeLease) State() kubeconn.State { return l.state }

func (l *fakeLease) Departed() bool { return l.departed }

func (l *fakeLease) WatchState() kubeconn.StateSubscription {
	panic("a cluster pass reads, it does not watch")
}

func (l *fakeLease) Release() { l.svc.released = append(l.svc.released, l.contextName) }

// probedAt is when every fake probe landed. Fixed, so a test asserting the stamp names a
// value rather than reading the clock the code under test would.
var probedAt = time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)

// answering is a pool whose "prod" claim reached the server and read id, or failed with
// err. The shape most tests want; knowing builds anything else.
func answering(id kubeconn.Identity, err error) *fakeKubeconn {
	if err != nil {
		return knowing(kubeconn.State{Connection: failed(err)})
	}
	// A part id leaves empty is left unanswered, which is what a probe that could not read
	// it reports.
	st := kubeconn.State{
		Connection: answeredWith("https://prod.example:6443"),
		Readiness:  answeredWith(kubeconn.ComponentStatus{}),
	}
	if id.ServerUID != "" {
		st.ServerUID = answeredWith(id.ServerUID)
	}
	if id.ServerVersion != "" {
		st.ServerVersion = answeredWith(kubeconn.VersionInfo{GitVersion: id.ServerVersion})
	}
	if id.Username != "" {
		st.Principal = answeredWith(kubeconn.Principal{Username: id.Username})
	}
	return knowing(st)
}

// answeredWith is a check that answered v at probedAt.
func answeredWith[T any](v T) kubeconn.Observation[T] {
	return kubeconn.Observation[T]{
		Value: v,
		Attempts: kubeconn.Attempts{
			LastSeen:    probedAt,
			LastAttempt: finished(kubeconn.ReasonSucceeded, ""),
		},
	}
}

// failed is a check whose last attempt did not answer.
func failed(err error) kubeconn.Observation[string] {
	return kubeconn.Observation[string]{
		Attempts: kubeconn.Attempts{
			LastAttempt: finished(kubeconn.ReasonUnreachable, err.Error()),
			Failures:    1, FailingSince: probedAt,
		},
	}
}

// finished is an attempt that ran and ended at probedAt, which is what makes its reason
// readable. The verdict follows the reason, the way a real probe's result would set it.
func finished(reason kubeconn.Reason, msg string) kubeconn.Attempt {
	verdict := kubeconn.VerdictFailed
	if reason == kubeconn.ReasonSucceeded {
		verdict = kubeconn.VerdictSucceeded
	}
	a := kubeconn.Attempt{
		ScheduledAt: probedAt, StartedAt: probedAt, FinishedAt: probedAt,
		Verdict: verdict, Reason: reason, Message: msg,
	}
	if msg != "" {
		a.Err = errors.New(msg)
	}
	return a
}

// knowing is a pool whose "prod" claim holds exactly this state.
func knowing(state kubeconn.State) *fakeKubeconn {
	return &fakeKubeconn{states: map[string]kubeconn.State{"prod": state}}
}

// newTestDepsAndBeehive is newTestDeps plus the beehive behind it, for a test that has
// to write what only a pass can — see newClusterStatusDeps.
func newTestDepsAndBeehive(t *testing.T) (deps, *beehive.Beehive) {
	t.Helper()
	bh := newTestBeehive(t)
	d := newDeps(bh, newTestKubeconfig(t), &fakeKubeconn{}, nil)

	_, err := registerControllers(bh, d)
	require.NoError(t, err)

	// The anchors the real service creates at startup: clusterController.Reconcile
	// reads one to declare its dependency edge.
	require.NoError(t, ensureClusterSources(context.Background(), d.sourceClient))
	return d, bh
}

// newClusterStatusDeps returns the shared set plus the client that stores a Cluster
// status from outside a pass, standing in for the connection probe: a fixture needs a
// stored status because the reconciles under test re-read it. Beehive is never run, so
// nothing reconciles behind these tests — which is also the only state an admin write
// is safe in.
func newClusterStatusDeps(t *testing.T) (deps, *beehive.AdminClient[ClusterStatus]) {
	t.Helper()
	bh := newTestBeehive(t)
	return newDeps(bh, newTestKubeconfig(t), &fakeKubeconn{}, nil), beehive.NewAdminClient[ClusterStatus](bh, ClusterGroupKind)
}

// newTestKubeconfig returns a started kubeconfig service over an empty temp dir, so
// it has read and every context is absent. Started because a reconcile defers until
// the first read, which Start does synchronously.
func newTestKubeconfig(t *testing.T) *kubeconfig.Service {
	t.Helper()
	svc := kubeconfig.New(filepath.Join(t.TempDir(), "config"), nil)

	stop, err := svc.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, svc.Close())
	})
	return svc
}

// newRunningBeehive is newTestBeehive started, so a watch's tail is live. No
// controller is registered: a reconcile writing status would put frames on a watch
// that the test never asked for.
func newRunningBeehive(t *testing.T, opts ...beehive.Option) *beehive.Beehive {
	t.Helper()
	bh := newTestBeehive(t, opts...)

	stop, err := bh.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	return bh
}

// newRunningDeps is the shared set over a running beehive, for the watch tests:
// without one the tail is dead and no change is ever reported. Nothing reconciles, so
// every frame is the test's own doing.
func newRunningDeps(t *testing.T, opts ...beehive.Option) deps {
	t.Helper()
	return newDeps(newRunningBeehive(t, opts...), newTestKubeconfig(t), &fakeKubeconn{}, nil)
}

// fakeKubeconfigSource is a hub the test publishes into, standing in for the
// watcher: same current-on-subscribe contract, driven by hand.
type fakeKubeconfigSource struct{ hub *watch.Hub[*api.Config] }

func newFakeKubeconfigSource(initial *api.Config) *fakeKubeconfigSource {
	return &fakeKubeconfigSource{hub: watch.New(initial)}
}

func (f *fakeKubeconfigSource) Subscribe() kubeconfig.Subscription { return f.hub.Receiver() }

// publish pushes a new snapshot to every subscriber.
func (f *fakeKubeconfigSource) publish(cfg *api.Config) { f.hub.Sender().Send(cfg) }

// close ends every subscription, standing in for the watcher shutting down.
func (f *fakeKubeconfigSource) close() { f.hub.Close() }

// cfgWith builds a snapshot holding one context per name, each naming its own cluster
// and user entry.
func cfgWith(names ...string) *api.Config {
	cfg := &api.Config{Contexts: map[string]*api.Context{}}
	for _, n := range names {
		cfg.Contexts[n] = &api.Context{Cluster: n + "-cluster", AuthInfo: n + "-user"}
	}
	return cfg
}

// cfgCurrent is cfgWith with one of the contexts marked current.
func cfgCurrent(current string, names ...string) *api.Config {
	cfg := cfgWith(names...)
	cfg.CurrentContext = current
	return cfg
}

// forbidden is a check the server answered and refused, which is a grant to fix rather
// than an outage to wait out.
func forbidden(msg string) kubeconn.Observation[string] {
	return kubeconn.Observation[string]{
		Attempts: kubeconn.Attempts{
			LastAttempt: finished(kubeconn.ReasonForbidden, msg),
			Failures:    1, FailingSince: probedAt,
		},
	}
}
