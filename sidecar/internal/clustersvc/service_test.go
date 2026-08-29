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

package clustersvc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// newTestService builds a service over a temp dir, closed on cleanup.
func newTestService(t *testing.T) Service {
	t.Helper()
	svc, err := New(filepath.Join(t.TempDir(), "data"), newTestKubeconfig(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, svc.Close()) })
	return svc
}

// New is the first thing the composition root builds, so nothing else has made
// dataDir yet — it has to create it rather than assume it.
func TestNewCreatesDataDir(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "nested", "data")
	svc, err := New(dataDir, newTestKubeconfig(t), nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, svc.Close()) }()

	assert.FileExists(t, filepath.Join(dataDir, "beehive.db"))
}

// A dataDir that cannot be created is a startup failure, not a service that limps
// along writing nowhere.
func TestNewRejectsAnUnusableDataDir(t *testing.T) {
	// A regular file where the directory should go: MkdirAll fails on it.
	blocked := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocked, nil, 0o600))

	_, err := New(filepath.Join(blocked, "data"), newTestKubeconfig(t), nil)
	assert.Error(t, err)
}

// The accessors are stateless views onto the one service, so each returns something
// usable and none of them is the nil interface.
func TestFamilyAccessors(t *testing.T) {
	svc := newTestService(t)

	assert.NotNil(t, svc.Clusters())
	assert.NotNil(t, svc.Caches())
	assert.NotNil(t, svc.CachedKinds())
	assert.NotNil(t, svc.CachedData())
}

// The full lifecycle the composition root drives: Start, the stop func it returns,
// then Close. Anything left running here would outlive the process's drain.
func TestStartStopClose(t *testing.T) {
	svc, err := New(filepath.Join(t.TempDir(), "data"), newTestKubeconfig(t), nil)
	require.NoError(t, err)

	stop, err := svc.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")

	assert.NoError(t, svc.Close())
}

// A dataDir that exists but cannot hold the store is a startup failure, not a
// service running against a database it never opened.
func TestNewRejectsAnUnopenableStore(t *testing.T) {
	dataDir := t.TempDir()
	// A directory where the database file goes: sqlite cannot open it.
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "beehive.db"), 0o700))

	_, err := New(dataDir, newTestKubeconfig(t), nil)
	assert.ErrorContains(t, err, "open beehive store")
}

// beehive rejects a second controller for a kind, and that error has to reach New's
// caller rather than leaving a kind silently unreconciled.
func TestRegisterControllersRejectsADuplicateKind(t *testing.T) {
	bh := newTestBeehive(t)

	_, err := registerControllers(bh, deps{})
	require.NoError(t, err)

	_, err = registerControllers(bh, deps{})
	assert.Error(t, err)
}

// Start is not re-entrant: beehive refuses a second start, and the failure must
// surface rather than half-starting the controllers behind it.
func TestStartRejectsASecondStart(t *testing.T) {
	svc := newTestService(t)
	stop, err := svc.Start(context.Background())
	require.NoError(t, err)
	defer func() { assert.NoError(t, stop(context.Background())) }()

	// kubesync is the first part in the order that refuses one; beehive refuses too, and the
	// unwind never reaches it.
	_, err = svc.Start(context.Background())
	assert.ErrorContains(t, err, "start kubesync")
}

// partNamed returns the service's part with that name.
func partNamed(t *testing.T, svc *service, name string) lifecycle.Part {
	t.Helper()
	i := slices.IndexFunc(svc.parts, func(p lifecycle.Part) bool { return p.Name == name })
	require.GreaterOrEqual(t, i, 0, "no part named %q", name)
	return svc.parts[i]
}

// failingPart refuses to start, so a test can drive the unwind path.
type failingPart struct{ err error }

func (f failingPart) Start(context.Context) (func(context.Context) error, error) {
	return nil, f.err
}

func (failingPart) Close() error { return nil }

// beehive starts before the controllers, so a controller that fails to start leaves
// it running with no stop func to reach it. Start has to drain it on the way out.
func TestStartDrainsBeehiveWhenAControllerFails(t *testing.T) {
	boom := errors.New("boom")
	svc := newTestService(t).(*service)
	// Beehive first, then a part that fails: by name, since a position moves whenever
	// something is added ahead of it.
	svc.parts = []lifecycle.Part{partNamed(t, svc, "beehive"), {Name: "a", StartCloser: failingPart{err: boom}}}

	_, err := svc.Start(context.Background())
	require.ErrorIs(t, err, boom)

	// beehive refuses to restart once stopped, which is how a drained instance is
	// told apart from one still running.
	_, err = svc.Start(context.Background())
	assert.ErrorContains(t, err, "already stopped")
}

// The rebuild's remaining surface, called through the boundary the resolvers use.
// Every entry must be deleted as its method lands: a stub that stops panicking fails
// here, which is what keeps this list honest about what is left.
func TestUnimplementedBoundaryPanics(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	var id ObjectID = 1

	calls := map[string]func(){
		"ListEvents":  func() { svc.ListEvents(ctx, id, nil, nil) },
		"WatchEvents": func() { svc.WatchEvents(ctx, id, nil) },
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) { assert.Panics(t, call) })
	}
}

// The bootstrap is a Part, so a store that cannot hold the anchors fails the start
// rather than leaving a service whose discovery pass has nothing to run against.
func TestStartFailsWhenTheAnchorsCannotBeCreated(t *testing.T) {
	part := clusterSourceBootstrap(deps{sourceClient: &stubSourceClient{createErr: errors.New("boom")}})

	_, err := lifecycle.StartAll(context.Background(), []lifecycle.Part{part})
	assert.ErrorContains(t, err, "cluster source records")
}

// --- the connection surface ---

// A claim names the record's context, which is what arms the probe behind it.
func TestAcquireConnectionClaimsTheRecordsContext(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	pool := d.kubeconnSvc.(*fakeKubeconn)

	lease, err := serviceOver(t, d).AcquireConnection(context.Background(), ClusterID(obj.ID))

	require.NoError(t, err)
	require.NotNil(t, lease)
	assert.Equal(t, []string{"prod"}, pool.asked)
}

// The claim is the caller's own: releasing it is the caller's business, and says nothing
// about the one clusterController holds to keep this cluster probed.
func TestAcquireConnectionHandsBackAReleasableClaim(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	pool := d.kubeconnSvc.(*fakeKubeconn)

	lease, err := serviceOver(t, d).AcquireConnection(context.Background(), ClusterID(obj.ID))
	require.NoError(t, err)
	lease.Release()

	assert.Equal(t, []string{"prod"}, pool.released)
}

func TestRetryConnectionReprobesTheRecordsContext(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	pool := d.kubeconnSvc.(*fakeKubeconn)

	require.NoError(t, serviceOver(t, d).RetryConnection(context.Background(), ClusterID(obj.ID)))

	assert.Equal(t, []string{"prod"}, pool.retried)
	assert.Empty(t, pool.asked, "a retry reaches the probe, not the claim")
}

// The gate is one rule, asserted over every method that goes through it: a caller must
// not find one of them willing to act on a record the others refuse.
func TestTheConnectionSurfaceRefusesTheSameRecords(t *testing.T) {
	tests := map[string]struct {
		record func(t *testing.T, d deps) ClusterID
		want   error
	}{
		"an id naming nothing": {
			record: func(*testing.T, deps) ClusterID { return ClusterID(404) },
			want:   ErrNotFound,
		},
		"a disabled cluster": {
			record: func(t *testing.T, d deps) ClusterID {
				obj, err := d.clusterClient.Create(context.Background(), KubeconfigName("prod"), ClusterSpec{
					Source: ClusterSpecSource{Kubeconfig: kubeconfigSrc("prod")},
				})
				require.NoError(t, err)
				return ClusterID(obj.ID)
			},
			want: ErrNotConnectable,
		},
		"a cluster awaiting deletion": {
			record: func(t *testing.T, d deps) ClusterID {
				obj := createCluster(t, d.clusterClient, "prod")
				require.NoError(t, d.clusterClient.Delete(context.Background(), obj.ID))
				return ClusterID(obj.ID)
			},
			want: ErrNotConnectable,
		},
		"a cluster from a source with no credentials": {
			record: func(t *testing.T, d deps) ClusterID {
				obj, err := d.clusterClient.Create(context.Background(), "adopted", ClusterSpec{Enabled: true})
				require.NoError(t, err)
				return ClusterID(obj.ID)
			},
			want: ErrNotConnectable,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d := newTestDeps(t)
			id := tt.record(t, d)
			svc := serviceOver(t, d)
			pool := d.kubeconnSvc.(*fakeKubeconn)

			_, acquireErr := svc.AcquireConnection(context.Background(), id)
			retryErr := svc.RetryConnection(context.Background(), id)
			_, scheduleErr := svc.Clusters().WatchSchedule(context.Background(), id)

			assert.ErrorIs(t, acquireErr, tt.want)
			assert.ErrorIs(t, retryErr, tt.want)
			assert.ErrorIs(t, scheduleErr, tt.want)
			assert.Empty(t, pool.asked)
			assert.Empty(t, pool.retried)
		})
	}
}

// A cluster nothing can reach is still claimed and still retried: whether the server
// answers is the probe's to report, not the record's to refuse.
func TestTheConnectionSurfaceServesAnUnreachableCluster(t *testing.T) {
	d := newTestDeps(t)
	obj := createCluster(t, d.clusterClient, "prod")
	d.kubeconnSvc = knowing(kubeconn.State{Connection: failed(errors.New("no route to host"))})
	pool := d.kubeconnSvc.(*fakeKubeconn)
	svc := serviceOver(t, d)

	lease, err := svc.AcquireConnection(context.Background(), ClusterID(obj.ID))
	require.NoError(t, err)
	require.NoError(t, svc.RetryConnection(context.Background(), ClusterID(obj.ID)))

	assert.False(t, lease.State().Connection.OK())
	assert.Equal(t, []string{"prod"}, pool.retried)
}

// The sync seam is a lifecycle participant like every other, and its place in the order is the
// contract: no pass can arm a cache that is stopping, and no worker outlives the store it writes.
func TestKubesyncStopsBetweenBeehiveAndTheStore(t *testing.T) {
	svc, err := New(t.TempDir(), newTestKubeconfig(t), poke.New())
	require.NoError(t, err)

	var names []string
	for _, part := range svc.(*service).parts {
		names = append(names, part.Name)
	}
	require.Subset(t, names, []string{"kubestore", "kubesync", "beehive"})
	assert.Less(t, slices.Index(names, "kubestore"), slices.Index(names, "kubesync"),
		"kubesync starts after the store it writes through, so it stops before it")
	assert.Less(t, slices.Index(names, "kubesync"), slices.Index(names, "beehive"),
		"beehive starts last and stops first, so no pass arms a cache that is stopping")
}

// A watch that died under a sleeping machine reports nothing, so a resume is the only thing that
// brings it back.
func TestAResumePokeRestartsEverySync(t *testing.T) {
	ctx := context.Background()
	pokeSvc := poke.New()
	sync := newFakeKubesync()

	stop, err := restartSyncsOnResume(sync, pokeSvc).StartCloser.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(ctx)) })

	pokeSvc.Poke(poke.SourceHost)
	testutil.Wait(t, sync.restarted.Chan(), "every sync to be restarted")
}
