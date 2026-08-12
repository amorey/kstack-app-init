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

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// newTestImporter builds a KubeconfigImporter against a fresh beehive
// (no-op controllers — the importer only writes Cluster specs).
func newTestImporter(t *testing.T, cfg *api.Config) *KubeconfigImporter {
	im, _ := newTestImporterWithCC(t, cfg)
	return im
}

// newTestImporterWithCC also hands back the Cluster kind's ControllerClient, for a test
// that needs to clear a finalizer the way a controller would.
func newTestImporterWithCC(t *testing.T, cfg *api.Config) (*KubeconfigImporter, beehive.ControllerClient[domain.ClusterStatus]) {
	t.Helper()
	bh, cc := NewTestBeehiveWithClusterCC(t)
	coreClient := beehive.NewClient[domain.ClusterSpec, domain.ClusterStatus](bh, domain.ClusterGroupKind)
	w := NewStaticWatcher(t, cfg)
	return NewKubeconfigImporter(w, coreClient), cc
}

// The importer creates a Cluster carrying only the source reference; the
// observation (cluster/user names, presence, isDefault) is the controller's job
// and is not written by the importer.
func TestImporterNewContextCreatesCluster(t *testing.T) {
	ctx := context.Background()
	cfg := testKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	obj := mustSingleObj(t, im.ClusterClient())
	require.NotNil(t, obj.Spec.Source.Kubeconfig)
	assert.Equal(t, "alpha", obj.Spec.Source.Kubeconfig.Context)
	assert.True(t, obj.Spec.Enabled)
	assert.True(t, obj.Spec.SyncEnabled)
	// The name is the source's natural key — the context name under the
	// kubeconfig source prefix. It is the importer's reconcile/uniqueness key, not
	// the record's identity (that is the beehive ObjectID).
	assert.Equal(t, "kubeconfig/alpha", obj.Name)
}

// Distinct contexts get distinct, deterministic source-prefixed names.
func TestImporterNameIsDeterministicNaturalKey(t *testing.T) {
	ctx := context.Background()
	cfg := testKubeConfig("alpha", "beta")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	objs, err := im.ClusterClient().List(ctx)
	require.NoError(t, err)
	names := map[string]bool{}
	for _, o := range objs {
		names[o.Name] = true
	}
	assert.True(t, names["kubeconfig/alpha"])
	assert.True(t, names["kubeconfig/beta"])
}

// A departed context is never deleted by the importer — the Cluster (and its
// owned cache) survives. Flipping it to not-present is the controller's job.
func TestImporterDepartedContextKeepsCluster(t *testing.T) {
	ctx := context.Background()
	cfg := testKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	origID := mustSingleObj(t, im.ClusterClient()).ID

	// Remove context from config: the importer must not delete the record.
	empty := &api.Config{Contexts: map[string]*api.Context{}}
	require.NoError(t, im.ReconcileClusterSet(ctx, empty))

	obj := mustSingleObj(t, im.ClusterClient())
	assert.Equal(t, origID, obj.ID, "departed context must not be deleted")
}

// A returning context reuses its (never-deleted) Cluster — the importer finds it
// by context and creates no duplicate.
func TestImporterReturningContextCreatesNoDuplicate(t *testing.T) {
	ctx := context.Background()
	cfg := testKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	origID := mustSingleObj(t, im.ClusterClient()).ID

	// Depart, then return.
	require.NoError(t, im.ReconcileClusterSet(ctx, &api.Config{Contexts: map[string]*api.Context{}}))
	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))

	obj := mustSingleObj(t, im.ClusterClient())
	assert.Equal(t, origID, obj.ID, "same object ID: reused in-place, no new cluster")
}

// An already-tracked context creates nothing on a repeat snapshot (no duplicate,
// no spec write — the importer only ever creates).
func TestImporterRepeatSnapshotCreatesNothing(t *testing.T) {
	ctx := context.Background()
	cfg := testKubeConfig("alpha")
	im := newTestImporter(t, cfg)

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj1 := mustSingleObj(t, im.ClusterClient())

	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	obj2 := mustSingleObj(t, im.ClusterClient())

	assert.Equal(t, obj1.ID, obj2.ID, "repeat snapshot must not create a second cluster")
	assert.Equal(t, obj1.Generation, obj2.Generation, "repeat snapshot must not bump generation")
}

func mustSingleObj(t *testing.T, cl beehive.Client[domain.ClusterSpec, domain.ClusterStatus]) *beehive.Object[domain.ClusterSpec, domain.ClusterStatus] {
	t.Helper()
	objs, err := cl.List(context.Background())
	require.NoError(t, err)
	require.Len(t, objs, 1)
	return objs[0]
}

// The import loop is driven by kubeconfig CHANGES, so an incomplete import has nothing
// behind it to finish the job. The case that matters is a user deleting a cluster: its
// name is held while the record drains, and if the import that lands in that window just
// gives up, the context is missing from the app until the user edits their kubeconfig or
// restarts the process.
func TestImporterRetriesAContextWhoseNameIsStillHeld(t *testing.T) {
	ctx := context.Background()
	cfg := testKubeConfig("alpha")
	im, cc := newTestImporterWithCC(t, cfg)

	// Occupy alpha's name with a record that is deletion-pending and staying that way.
	blocker, err := im.ClusterClient().Create(ctx, domain.KubeconfigName("alpha"),
		domain.ClusterSpec{Source: domain.ClusterSpecSource{Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: "alpha"}}},
		beehive.WithFinalizers("test.kstack.io/hold"))
	require.NoError(t, err)
	require.NoError(t, im.ClusterClient().Delete(ctx, blocker.ID))

	im.Start()
	t.Cleanup(im.Stop)

	// The first import can't create alpha, and no kubeconfig change is coming.
	require.Never(t, func() bool { return liveContexts(t, im)["alpha"] > 0 },
		200*time.Millisecond, 20*time.Millisecond)

	// Release the name. Only the retry can notice — nothing publishes a new snapshot.
	require.NoError(t, cc.DeleteFinalizer(ctx, blocker.ID, "test.kstack.io/hold"))

	require.Eventually(t, func() bool { return liveContexts(t, im)["alpha"] == 1 },
		4*importRetryInterval, 20*time.Millisecond,
		"the importer must retry a name it could not take, not wait for a kubeconfig edit")
}

// liveContexts counts the non-deleting Clusters per kubeconfig context.
func liveContexts(t *testing.T, im *KubeconfigImporter) map[string]int {
	t.Helper()
	objs, err := im.ClusterClient().List(context.Background())
	require.NoError(t, err)
	live := map[string]int{}
	for _, obj := range objs {
		if obj.DeletionRequestedAt == nil && obj.Spec.Source.Kubeconfig != nil {
			live[obj.Spec.Source.Kubeconfig.Context]++
		}
	}
	return live
}

// A Cluster being deleted still HOLDS its name — tracked ignores deletion-pending objects
// on purpose, so a re-created context gets a fresh record, but beehive rejects the create
// with ErrNameTaken until GC collects the row.
//
// That must not abort the snapshot. Contexts are iterated out of a map, so returning on the
// first error skipped an arbitrary, run-to-run-varying subset of the OTHER contexts: a
// kubeconfig snapshot landing during a delete's drain window could leave whole clusters
// unimported until something else triggered a reimport.
func TestImporterSkipsADeletingContextWithoutAbandoningTheRest(t *testing.T) {
	ctx := context.Background()
	cfg := testKubeConfig("alpha", "beta", "gamma", "delta")
	im := newTestImporter(t, cfg)

	// Beta is created here rather than by the import below, so it can carry a finalizer
	// nothing clears. Deleting an object with no finalizer is a race against GC: it may be
	// collected before the second import, which then simply re-creates it and never
	// exercises the ErrNameTaken branch this test is about.
	beta, err := im.ClusterClient().Create(ctx, domain.KubeconfigName("beta"),
		domain.ClusterSpec{
			SyncEnabled: true,
			Enabled:     true,
			Source:      domain.ClusterSpecSource{Kubeconfig: &domain.ClusterSpecSourceKubeconfig{Context: "beta"}},
		},
		beehive.WithFinalizers("test.kstack.io/hold"))
	require.NoError(t, err)

	// Import the rest, then request beta's deletion — its name stays taken while it drains.
	require.NoError(t, im.ReconcileClusterSet(ctx, cfg))
	objs, err := im.ClusterClient().List(ctx)
	require.NoError(t, err)
	require.Len(t, objs, 4)

	require.NoError(t, im.ClusterClient().Delete(ctx, beta.ID))
	held, err := im.ClusterClient().Get(ctx, beta.ID)
	require.NoError(t, err)
	require.NotNil(t, held.DeletionRequestedAt, "beta must still be draining, holding its name")

	// A fresh snapshot: beta is deletion-pending, so it is untracked and its create fails.
	// Reported, not swallowed — the import is incomplete and this loop is driven by
	// kubeconfig changes, so nothing else would ever finish it — but named apart from a
	// real failure, since nothing is wrong.
	err = im.ReconcileClusterSet(ctx, cfg)
	require.ErrorIs(t, err, errNameHeld)
	require.Contains(t, err.Error(), "beta")

	// Every other context must still be tracked exactly once.
	after, err := im.ClusterClient().List(ctx)
	require.NoError(t, err)
	live := map[string]int{}
	for _, obj := range after {
		if obj.DeletionRequestedAt == nil {
			live[obj.Spec.Source.Kubeconfig.Context]++
		}
	}
	assert.Equal(t, map[string]int{"alpha": 1, "gamma": 1, "delta": 1}, live,
		"one deleting context must not cost the others their import")
}
