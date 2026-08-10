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

package objectsync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// These tests exercise the objects write path against a real (temp-dir) cache db — the
// same file the worker writes into in production. The list/watch state machine that
// drives it is kubesync's, and tested there.

var deploymentsKind = Kind{
	APIVersion: "apps/v1",
	Kind:       "Deployment",
	Resource:   "deployments",
	Namespaced: true,
}

func openTestCache(t *testing.T) *store.ClusterDB {
	t.Helper()
	mgr := store.NewManager(t.TempDir())
	cdb, err := mgr.Open(context.Background(), store.CacheRef{ClusterID: 1, CacheID: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })
	return cdb
}

// newDeployment builds a Deployment body carrying everything the write path projects:
// identity, an owner reference, labels, and the two pieces of server noise that must be
// stripped before the body is stored.
func newDeployment(uid, name string, opts ...func(map[string]any)) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"uid":               uid,
			"name":              name,
			"namespace":         "default",
			"resourceVersion":   "10",
			"generation":        int64(2),
			"creationTimestamp": "2026-08-07T10:00:00Z",
			"labels":            map[string]any{"app": "web", "tier": "front"},
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": `{"spec":{}}`,
				"keep-me": "yes",
			},
			"managedFields": []any{map[string]any{"manager": "kubectl"}},
			"ownerReferences": []any{map[string]any{
				"uid":        "owner-uid",
				"kind":       "Rollout",
				"controller": true,
			}},
		},
		"spec": map[string]any{"replicas": int64(3)},
	}
	for _, o := range opts {
		o(obj)
	}
	return &unstructured.Unstructured{Object: obj}
}

// storedBody reads back one object's raw_json, decompressed.
func storedBody(t *testing.T, cdb *store.ClusterDB, uid string) map[string]any {
	t.Helper()
	var raw []byte
	require.NoError(t, cdb.Reader().QueryRow(`SELECT raw_json FROM objects WHERE uid=?`, uid).Scan(&raw))
	body, err := store.DecompressRaw(raw)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func ownerUIDs(t *testing.T, cdb *store.ClusterDB, uid string) []string {
	t.Helper()
	rows, err := cdb.Reader().Query(`SELECT owner_uid FROM owner_refs WHERE child_uid=?`, uid)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

func labelKeys(t *testing.T, cdb *store.ClusterDB, uid string) []string {
	t.Helper()
	rows, err := cdb.Reader().Query(`SELECT key FROM labels WHERE uid=?`, uid)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

func uidsOfKind(t *testing.T, cdb *store.ClusterDB, kind string) []string {
	t.Helper()
	rows, err := cdb.Reader().Query(`SELECT uid FROM objects WHERE kind=? ORDER BY uid`, kind)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

// TestApplyWritesIdentityAndStrippedBody covers the row one Added delta produces: the
// universal identity columns the dashboard reads, and a body with the server noise gone.
func TestApplyWritesIdentityAndStrippedBody(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("uid-1", "web")))

	var apiVersion, kind, ns, name, rv string
	var generation, createdAt int64
	require.NoError(t, cdb.Reader().QueryRow(
		`SELECT api_version, kind, namespace, name, resource_version, generation, created_at
		 FROM objects WHERE uid=?`, "uid-1").
		Scan(&apiVersion, &kind, &ns, &name, &rv, &generation, &createdAt))
	assert.Equal(t, "apps/v1", apiVersion)
	assert.Equal(t, "Deployment", kind)
	assert.Equal(t, "default", ns)
	assert.Equal(t, "web", name)
	assert.Equal(t, "10", rv)
	assert.Equal(t, int64(2), generation)
	assert.NotZero(t, createdAt, "creationTimestamp must be stored so the table can show an age")

	body := storedBody(t, cdb, "uid-1")
	meta, _ := body["metadata"].(map[string]any)
	assert.NotContains(t, meta, "managedFields", "managedFields is server noise and roughly halves the stored body")
	annotations, _ := meta["annotations"].(map[string]any)
	assert.NotContains(t, annotations, "kubectl.kubernetes.io/last-applied-configuration",
		"the last-applied annotation duplicates the whole spec")
	assert.Equal(t, "yes", annotations["keep-me"], "only the noise is stripped, not the object's own annotations")
	assert.Equal(t, map[string]any{"replicas": float64(3)}, body["spec"], "the rest of the body is stored verbatim")
}

// TestApplyMaterializesOwnersAndLabels covers the graph edges the schema exists for: the
// agent walks ownership and label selectors with JOINs instead of parsing every body.
func TestApplyMaterializesOwnersAndLabels(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("uid-1", "web")))
	assert.Equal(t, []string{"owner-uid"}, ownerUIDs(t, cdb, "uid-1"))
	assert.ElementsMatch(t, []string{"app", "tier"}, labelKeys(t, cdb, "uid-1"))

	// An update REPLACES the edges rather than accumulating them: a relabelled object
	// whose old rows survived would keep matching a selector it no longer satisfies.
	relabelled := newDeployment("uid-1", "web", func(o map[string]any) {
		meta := o["metadata"].(map[string]any)
		meta["labels"] = map[string]any{"app": "web"}
		delete(meta, "ownerReferences")
	})
	require.NoError(t, st.ApplyChange(ctx, watch.Modified, relabelled))
	assert.Empty(t, ownerUIDs(t, cdb, "uid-1"), "a dropped ownerReference must not linger")
	assert.Equal(t, []string{"app"}, labelKeys(t, cdb, "uid-1"))
}

// TestApplyDeleteRemovesObjectAndEdges pins that a deletion takes the whole object with
// it — a surviving owner_refs or labels row would be an edge pointing at nothing.
func TestApplyDeleteRemovesObjectAndEdges(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("uid-1", "web")))
	require.NoError(t, st.ApplyChange(ctx, watch.Deleted, newDeployment("uid-1", "web")))

	assert.Empty(t, uidsOfKind(t, cdb, "Deployment"))
	assert.Empty(t, ownerUIDs(t, cdb, "uid-1"))
	assert.Empty(t, labelKeys(t, cdb, "uid-1"))
}

// TestApplyRedactsSecretData pins that Secret values never reach the disk. The cache is a
// plaintext file on the user's machine, and a mirrored Secret would put every credential
// in the cluster into it.
func TestApplyRedactsSecretData(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	secrets := Kind{APIVersion: "v1", Kind: "Secret", Resource: "secrets", Namespaced: true}
	st := newObjectStore(cdb, secrets)

	secret := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"uid": "sec-1", "name": "db", "namespace": "default", "resourceVersion": "1"},
		"type":       "Opaque",
		"data":       map[string]any{"password": "aHVudGVyMg=="},
		"stringData": map[string]any{"token": "s3cret"},
	}}
	require.NoError(t, st.ApplyChange(ctx, watch.Added, secret))

	body := storedBody(t, cdb, "sec-1")
	assert.NotContains(t, body, "stringData", "stringData is dropped outright")
	data, _ := body["data"].(map[string]any)
	assert.Equal(t, map[string]any{"password": redactedValue}, data,
		"the keys stay visible so the UI can list them; only the values go")
	assert.Equal(t, "Opaque", body["type"], "the rest of the Secret is untouched")
}

// TestReplacePrunesOnlyThisKind is the reason the objects table's shared-ness matters: a
// full relist reconciles the server's answer for ONE kind, and must leave every other
// kind's rows — written by their own workers, into the same table — alone.
func TestReplacePrunesOnlyThisKind(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	deployments := newObjectStore(cdb, deploymentsKind)
	pods := newObjectStore(cdb, Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true})

	require.NoError(t, deployments.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "web")))
	require.NoError(t, deployments.ApplyChange(ctx, watch.Added, newDeployment("dep-2", "api")))
	require.NoError(t, pods.ApplyChange(ctx, watch.Added, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"uid": "pod-1", "name": "web-1", "namespace": "default", "resourceVersion": "1"},
	}}))

	// The server now serves only dep-1.
	sess := deployments.BeginReplace()
	require.NoError(t, sess.WritePage(ctx, []*unstructured.Unstructured{newDeployment("dep-1", "web")}))
	require.NoError(t, commitErr(sess.Commit(ctx, "500")))

	assert.Equal(t, []string{"dep-1"}, uidsOfKind(t, cdb, "Deployment"), "dep-2 is gone from the server, so from the cache")
	assert.Equal(t, []string{"pod-1"}, uidsOfKind(t, cdb, "Pod"), "another kind's rows are not this prune's business")

	rv, err := deployments.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Equal(t, "500", rv, "a completed relist leaves the cookie the next watch resumes from")
}

// TestReplaceFirstPageClearsCookie pins the ordering that makes a half-written relist
// safe: the cookie dies with the first page's rows, so a pass that dies mid-pagination
// can never leave rows behind a cookie that would resume past them.
func TestReplaceFirstPageClearsCookie(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)
	require.NoError(t, st.PersistRV(ctx, "42"))

	sess := st.BeginReplace()
	require.NoError(t, sess.WritePage(ctx, []*unstructured.Unstructured{newDeployment("dep-1", "web")}))

	rv, err := st.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Empty(t, rv, "a relist in flight must leave no cookie for the next start to trust")
}

// TestEnsureCatalogRegistersKind covers what makes a synced kind readable: the
// kind_catalog row is how store.Objects translates the plural resource a watch is opened
// on back to the Kind the objects table is keyed by, and it's what the dashboard nav lists.
func TestEnsureCatalogRegistersKind(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)

	require.NoError(t, st.EnsureCatalog(ctx))
	require.NoError(t, st.EnsureCatalog(ctx), "re-registering on every worker start must be a no-op")
	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "web")))

	kinds, err := cdb.Kinds(ctx)
	require.NoError(t, err)
	require.Len(t, kinds, 1)
	assert.Equal(t, "apps/v1", kinds[0].APIVersion)
	assert.Equal(t, "Deployment", kinds[0].Kind)
	assert.Equal(t, "deployments", kinds[0].Resource)
	assert.Equal(t, "Namespaced", kinds[0].Scope)
	assert.False(t, kinds[0].IsCRD)
	assert.Equal(t, 1, kinds[0].Count)

	// With the catalog row in place the read side can resolve the kind by its plural.
	objs, err := cdb.Objects(ctx, "apps/v1", "deployments")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	assert.Equal(t, "web", objs[0].Name)
}

// TestForgetDropsEverythingForTheKind covers the deletion path: when a kind stops being
// synced, its rows, its catalog entry and its cookie all go, so the cache never advertises
// a kind whose contents are frozen at whenever the sync stopped.
func TestForgetDropsEverythingForTheKind(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)
	require.NoError(t, st.EnsureCatalog(ctx))
	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "web")))
	require.NoError(t, st.PersistRV(ctx, "42"))

	require.NoError(t, st.Forget(ctx))

	assert.Empty(t, uidsOfKind(t, cdb, "Deployment"))
	assert.Empty(t, ownerUIDs(t, cdb, "dep-1"))
	kinds, err := cdb.Kinds(ctx)
	require.NoError(t, err)
	assert.Empty(t, kinds)
	rv, err := st.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Empty(t, rv)
}

// TestWriteNotifiesOnlyItsResource pins the routing the per-kind objects watch depends
// on: a Deployment write must not wake a watch subscribed to Pods (which would re-read
// and diff for nothing), while the keyless kind-catalog watch still wakes on both.
func TestWriteNotifiesOnlyItsResource(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)

	deploymentsC, cancelDeployments := cdb.ObjectsSubscribeResource("apps/v1", "deployments")
	defer cancelDeployments()
	podsC, cancelPods := cdb.ObjectsSubscribeResource("v1", "pods")
	defer cancelPods()
	anyC, cancelAny := cdb.ObjectsSubscribe()
	defer cancelAny()

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "web")))

	assert.True(t, notified(deploymentsC), "the kind's own watch must wake")
	assert.True(t, notified(anyC), "a keyless subscriber (the kind catalog) must wake on any write")
	assert.False(t, notified(podsC), "an unrelated kind's watch must not re-read")
}

// notified reports whether a write ping arrives on ch. The bus delivers a Chan
// subscriber through a feeder goroutine, so a ping is NOT already pending when the
// write returns — a select/default here would report false for every subscriber and
// the routing assertions would all pass vacuously.
func notified(ch <-chan store.WriteWake) bool {
	select {
	case <-ch:
		return true
	case <-time.After(200 * time.Millisecond):
		return false
	}
}

// TestReplacePrunesRowsWrittenInTheSameMillisecond pins the sweep boundary. updated_at has
// millisecond resolution, so a row written in the same tick the relist began carries the
// session's own start stamp — with an inclusive boundary it would masquerade as a row the
// pass wrote and survive a prune it deserved. A relist immediately after a write is the
// common shape (a 410 on a busy cluster), so this must not depend on the clock ticking.
func TestReplacePrunesRowsWrittenInTheSameMillisecond(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)

	// Freeze the clock so the write and the relist genuinely share a millisecond — left to
	// the real clock this passes or fails on whether the tick happened to land.
	st.now = func() int64 { return 1_700_000_000_000 }

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-gone", "gone")))

	sess := st.BeginReplace()
	require.NoError(t, sess.WritePage(ctx, []*unstructured.Unstructured{newDeployment("dep-kept", "kept")}))
	require.NoError(t, commitErr(sess.Commit(ctx, "500")))

	assert.Equal(t, []string{"dep-kept"}, uidsOfKind(t, cdb, "Deployment"),
		"a row the relist did not carry must be swept however recently it was written")
}

// withRV overrides the body's resourceVersion, so a test can model an object the cluster
// has moved on from.
func withRV(rv string) func(map[string]any) {
	return func(obj map[string]any) {
		obj["metadata"].(map[string]any)["resourceVersion"] = rv
	}
}

// SnapshotRVs is what lets the driver's resync ask "which of my rows moved?" without
// re-downloading every body. It must report this kind's rows and their resourceVersions —
// and only this kind's, since the objects table is shared with every other worker's.
func TestSnapshotRVsIsKindScoped(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	deployments := newObjectStore(cdb, deploymentsKind)
	pods := newObjectStore(cdb, Kind{
		APIVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true,
	})
	require.NoError(t, deployments.EnsureCatalog(ctx))
	require.NoError(t, pods.EnsureCatalog(ctx))

	require.NoError(t, deployments.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one")))
	require.NoError(t, deployments.ApplyChange(ctx, watch.Added, newDeployment("dep-2", "two", withRV("77"))))
	pod := newDeployment("pod-1", "p")
	pod.Object["apiVersion"], pod.Object["kind"] = "v1", "Pod"
	require.NoError(t, pods.ApplyChange(ctx, watch.Added, pod))

	got, err := deployments.SnapshotRVs(ctx)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{"dep-1": "10", "dep-2": "77"}, got,
		"another kind's rows belong to another worker's resync")
}

// DeleteByUIDs is the diff resync's prune — the counterpart to the relist's sweep, for the
// objects the cluster no longer has. It must take their edges with them, or a later
// selector query matches rows whose object is gone; and it must leave every other kind's
// rows alone, since the objects table is shared.
func TestDeleteByUIDsRemovesTheObjectsAndTheirEdges(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)
	require.NoError(t, st.EnsureCatalog(ctx))
	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one")))
	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-2", "two")))
	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-keep", "keep")))

	require.NoError(t, st.DeleteByUIDs(ctx, []string{"dep-1", "dep-2"}))

	assert.Equal(t, []string{"dep-keep"}, uidsOfKind(t, cdb, "Deployment"))
	assert.Empty(t, ownerUIDs(t, cdb, "dep-1"))
	assert.Empty(t, labelKeys(t, cdb, "dep-1"))
	assert.Empty(t, ownerUIDs(t, cdb, "dep-2"))

	// The cookie is untouched: a deletion carries no resourceVersion, and the pass
	// persists the list's RV once it has reconciled everything.
	rv, err := st.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Equal(t, "10", rv)
}

// An empty set must not open a transaction at all — the resync calls it on every pass,
// and the steady state is that nothing vanished.
func TestDeleteByUIDsIgnoresAnEmptySet(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)
	require.NoError(t, st.EnsureCatalog(ctx))
	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one")))

	require.NoError(t, st.DeleteByUIDs(ctx, nil))

	assert.Equal(t, []string{"dep-1"}, uidsOfKind(t, cdb, "Deployment"))
}

// A CRD recreated with the same (apiVersion, resource) but a new Kind is the same sync
// child respecified, so its worker is rebuilt against the new identity — and the rows it
// wrote under the old one have no owner left to collect them. Forget is what the controller
// calls to drop them; pin that it takes the whole trace of that kind and leaves every other
// kind's rows alone, since the objects table is shared.
func TestForgetDropsOnlyTheNamedKind(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)

	oldKind := deploymentsKind
	newKind := Kind{APIVersion: "apps/v1", Kind: "Rollout", Resource: "deployments", Namespaced: true}
	bystander := Kind{APIVersion: "v1", Kind: "Pod", Resource: "pods", Namespaced: true}

	before := newObjectStore(cdb, oldKind)
	require.NoError(t, before.EnsureCatalog(ctx))
	require.NoError(t, before.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one")))

	other := newObjectStore(cdb, bystander)
	require.NoError(t, other.EnsureCatalog(ctx))
	pod := newDeployment("pod-1", "p")
	pod.Object["apiVersion"], pod.Object["kind"] = "v1", "Pod"
	require.NoError(t, other.ApplyChange(ctx, watch.Added, pod))

	// The remap: forget the superseded identity, then sync under the new one.
	require.NoError(t, Forget(ctx, cdb, oldKind))
	after := newObjectStore(cdb, newKind)
	require.NoError(t, after.EnsureCatalog(ctx))

	assert.Empty(t, uidsOfKind(t, cdb, "Deployment"), "the superseded kind's rows must be gone")
	assert.Empty(t, ownerUIDs(t, cdb, "dep-1"))
	assert.Empty(t, labelKeys(t, cdb, "dep-1"))
	assert.Equal(t, []string{"pod-1"}, uidsOfKind(t, cdb, "Pod"), "another kind's rows are not ours to drop")

	// The catalog must describe what the cache now holds — one entry for this resource,
	// under the new Kind, or the dashboard's nav keeps a phantom row forever.
	kinds, err := cdb.Kinds(ctx)
	require.NoError(t, err)
	var deploymentsKinds []string
	for _, k := range kinds {
		if k.Resource == "deployments" {
			deploymentsKinds = append(deploymentsKinds, k.Kind)
		}
	}
	assert.Equal(t, []string{"Rollout"}, deploymentsKinds)
}

// The same rename with NOBODY to call Forget: the CRD's Kind changed while the sidecar was
// down, so the old worker was never running to clean up after itself. The registering
// worker has to do it, and dropping only the stale catalog row is not enough — its objects,
// their edges, its kind_counts row and its resume cookie are all keyed by the old Kind, and
// nothing will ever name that Kind again. Left behind they are unreachable and unreclaimable:
// the cache file carries them for the life of the cache.
func TestEnsureCatalogPurgesAKindRenamedWhileWeWereDown(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)

	oldKind := deploymentsKind
	newKind := Kind{APIVersion: "apps/v1", Kind: "Rollout", Resource: "deployments", Namespaced: true}

	before := newObjectStore(cdb, oldKind)
	require.NoError(t, before.EnsureCatalog(ctx))
	require.NoError(t, before.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "web")))
	require.NoError(t, before.PersistRV(ctx, "42"))

	// The restart: the new identity registers, with no Forget in between.
	after := newObjectStore(cdb, newKind)
	require.NoError(t, after.EnsureCatalog(ctx))

	assert.Empty(t, uidsOfKind(t, cdb, "Deployment"), "the renamed-away Kind's rows must go with it")
	assert.Empty(t, ownerUIDs(t, cdb, "dep-1"))
	assert.Empty(t, labelKeys(t, cdb, "dep-1"))

	rv, err := before.ResumeRV(ctx)
	require.NoError(t, err)
	assert.Empty(t, rv, "and its resume cookie, which nothing else would ever key on again")

	// The catalog and its per-kind counts describe only what the cache now holds.
	kinds, err := cdb.Kinds(ctx)
	require.NoError(t, err)
	require.Len(t, kinds, 1)
	assert.Equal(t, "Rollout", kinds[0].Kind)
	assert.Zero(t, kinds[0].Count)

	var stale int
	require.NoError(t, cdb.Reader().QueryRow(
		`SELECT COUNT(*) FROM kind_counts WHERE api_version=? AND kind=?`, "apps/v1", "Deployment").Scan(&stale))
	assert.Zero(t, stale, "the trigger-maintained tally must not outlive its kind")
}

// statusHistory reads one object's recorded transitions, oldest first.
func statusHistory(t *testing.T, cdb *store.ClusterDB, uid string) []string {
	t.Helper()
	rows, err := cdb.Reader().Query(`SELECT summary FROM status_history WHERE uid=? ORDER BY at, rowid`, uid)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

// The status timeline is what answers "when did this Pod start CrashLooping?" without
// log-scraping. It records TRANSITIONS: a relist rewrites every row whether or not anything
// moved, so an unconditional insert would bury the real changes under a copy of the whole
// collection every resync period.
func TestApplyRecordsStatusTransitionsOnly(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)
	require.NoError(t, st.EnsureCatalog(ctx))

	progressing := func(obj map[string]any) {
		obj["spec"] = map[string]any{"replicas": int64(3)}
		obj["status"] = map[string]any{"readyReplicas": int64(2)}
	}
	available := func(obj map[string]any) {
		obj["spec"] = map[string]any{"replicas": int64(3)}
		obj["status"] = map[string]any{"readyReplicas": int64(3)}
	}

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one", progressing)))
	// Same summary again — a heartbeat write, or a relist rewriting an unchanged row.
	require.NoError(t, st.ApplyChange(ctx, watch.Modified, newDeployment("dep-1", "one", progressing)))
	require.NoError(t, st.ApplyChange(ctx, watch.Modified, newDeployment("dep-1", "one", available)))

	assert.Equal(t, []string{"Progressing 2/3", "Available 3/3"}, statusHistory(t, cdb, "dep-1"),
		"only real transitions belong in the timeline")
}

// A kind this package can't summarize records nothing, rather than a run of empty strings.
func TestApplyRecordsNoHistoryWithoutASummary(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	crd := Kind{APIVersion: "acme.io/v1", Kind: "Widget", Resource: "widgets", Namespaced: true}
	st := newObjectStore(cdb, crd)
	require.NoError(t, st.EnsureCatalog(ctx))

	widget := newDeployment("w-1", "one")
	widget.Object["apiVersion"], widget.Object["kind"] = "acme.io/v1", "Widget"
	delete(widget.Object, "status")
	require.NoError(t, st.ApplyChange(ctx, watch.Added, widget))

	assert.Empty(t, statusHistory(t, cdb, "w-1"))
}

// Every path that removes an object must take its history with it, or the rows outlive the
// object and the janitor's TTL becomes the only thing that ever collects them.
func TestDeletePathsCascadeStatusHistory(t *testing.T) {
	ctx := context.Background()
	ready := func(obj map[string]any) {
		obj["spec"] = map[string]any{"replicas": int64(1)}
		obj["status"] = map[string]any{"readyReplicas": int64(1)}
	}

	t.Run("watch delete", func(t *testing.T) {
		cdb := openTestCache(t)
		st := newObjectStore(cdb, deploymentsKind)
		require.NoError(t, st.EnsureCatalog(ctx))
		require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one", ready)))
		require.NotEmpty(t, statusHistory(t, cdb, "dep-1"))

		require.NoError(t, st.ApplyChange(ctx, watch.Deleted, newDeployment("dep-1", "one", ready)))
		assert.Empty(t, statusHistory(t, cdb, "dep-1"))
	})

	t.Run("diff resync prune", func(t *testing.T) {
		cdb := openTestCache(t)
		st := newObjectStore(cdb, deploymentsKind)
		require.NoError(t, st.EnsureCatalog(ctx))
		require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one", ready)))

		require.NoError(t, st.DeleteByUIDs(ctx, []string{"dep-1"}))
		assert.Empty(t, statusHistory(t, cdb, "dep-1"))
	})

	t.Run("relist sweep", func(t *testing.T) {
		cdb := openTestCache(t)
		st := newObjectStore(cdb, deploymentsKind)
		require.NoError(t, st.EnsureCatalog(ctx))
		st.now = func() int64 { return 1_700_000_000_000 }
		require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-gone", "gone", ready)))

		sess := st.BeginReplace()
		require.NoError(t, sess.WritePage(ctx, []*unstructured.Unstructured{newDeployment("dep-kept", "kept", ready)}))
		require.NoError(t, commitErr(sess.Commit(ctx, "500")))

		assert.Empty(t, statusHistory(t, cdb, "dep-gone"))
		assert.NotEmpty(t, statusHistory(t, cdb, "dep-kept"))
	})

	t.Run("forget the kind", func(t *testing.T) {
		cdb := openTestCache(t)
		st := newObjectStore(cdb, deploymentsKind)
		require.NoError(t, st.EnsureCatalog(ctx))
		require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one", ready)))

		require.NoError(t, st.Forget(ctx))
		assert.Empty(t, statusHistory(t, cdb, "dep-1"))
	})
}

// ownerChildUIDs reads the edges pointing AT an owner — the direction a deleted object is
// on the far side of.
func ownerChildUIDs(t *testing.T, cdb *store.ClusterDB, ownerUID string) []string {
	t.Helper()
	rows, err := cdb.Reader().Query(`SELECT child_uid FROM owner_refs WHERE owner_uid=?`, ownerUID)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

// A deleted object is both a child (its own references out) and an owner (its children's
// references in). Only the first is obvious, and with --cascade=orphan the children outlive
// their owner — so leaving the inbound edges behind leaves rows pointing at a uid that no
// longer exists, which any owner-based lookup then trips over.
func TestDeletePathsClearBothEdgeDirections(t *testing.T) {
	ctx := context.Background()

	// An object that OWNS another: the deployment is owner-uid of its replicaset row.
	seed := func(t *testing.T, cdb *store.ClusterDB, st *objectStore) {
		t.Helper()
		require.NoError(t, st.EnsureCatalog(ctx))
		require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one")))
		_, err := cdb.Writer().ExecContext(ctx,
			`INSERT INTO owner_refs (child_uid, owner_uid, is_controller) VALUES ('rs-1', 'dep-1', 1)`)
		require.NoError(t, err)
		require.Equal(t, []string{"rs-1"}, ownerChildUIDs(t, cdb, "dep-1"))
	}

	t.Run("watch delete", func(t *testing.T) {
		cdb := openTestCache(t)
		st := newObjectStore(cdb, deploymentsKind)
		seed(t, cdb, st)

		require.NoError(t, st.ApplyChange(ctx, watch.Deleted, newDeployment("dep-1", "one")))
		assert.Empty(t, ownerChildUIDs(t, cdb, "dep-1"), "the edges pointing at the owner must go too")
	})

	t.Run("diff resync prune", func(t *testing.T) {
		cdb := openTestCache(t)
		st := newObjectStore(cdb, deploymentsKind)
		seed(t, cdb, st)

		require.NoError(t, st.DeleteByUIDs(ctx, []string{"dep-1"}))
		assert.Empty(t, ownerChildUIDs(t, cdb, "dep-1"))
	})

	t.Run("relist sweep", func(t *testing.T) {
		cdb := openTestCache(t)
		st := newObjectStore(cdb, deploymentsKind)
		// Freeze BEFORE seeding, so the seeded row is stamped below the sweep's mark and
		// is genuinely swept (the mark is one ms past the session start).
		st.now = func() int64 { return 1_700_000_000_000 }
		seed(t, cdb, st)

		sess := st.BeginReplace()
		require.NoError(t, sess.WritePage(ctx, []*unstructured.Unstructured{newDeployment("dep-kept", "kept")}))
		require.NoError(t, commitErr(sess.Commit(ctx, "500")))

		assert.Empty(t, ownerChildUIDs(t, cdb, "dep-1"))
	})
}

// creationTimestamp is immutable in Kubernetes, so a write that lacks one carries no news
// about it. project leaves the column 0 in that case, and an unconditional upsert wrote
// that zero over a good value — after which the dashboard's Age column reads 56 years.
func TestApplyKeepsCreatedAtWhenTheBodyHasNone(t *testing.T) {
	ctx := context.Background()
	cdb := openTestCache(t)
	st := newObjectStore(cdb, deploymentsKind)
	require.NoError(t, st.EnsureCatalog(ctx))

	require.NoError(t, st.ApplyChange(ctx, watch.Added, newDeployment("dep-1", "one")))
	var created int64
	require.NoError(t, cdb.Reader().QueryRow(`SELECT created_at FROM objects WHERE uid=?`, "dep-1").Scan(&created))
	require.NotZero(t, created)

	stripped := newDeployment("dep-1", "one", func(obj map[string]any) {
		delete(obj["metadata"].(map[string]any), "creationTimestamp")
	})
	require.NoError(t, st.ApplyChange(ctx, watch.Modified, stripped))

	var after int64
	require.NoError(t, cdb.Reader().QueryRow(`SELECT created_at FROM objects WHERE uid=?`, "dep-1").Scan(&after))
	assert.Equal(t, created, after, "a body with no creationTimestamp must not erase the recorded one")
}

// commitErr drops a replace session's prune count, for the tests that only care that the
// commit succeeded.
func commitErr(_ int, err error) error { return err }

// The items reaching a store come from a pluggable Source, so an empty body is the caller's
// to send and this package's to survive: GetUID on one panics, and the call runs inside a
// worker goroutine, which takes the process down with it. Both write paths reject it.
func TestWriteRejectsAnEmptyObject(t *testing.T) {
	ctx := context.Background()
	st := newObjectStore(openTestCache(t), deploymentsKind)

	for name, u := range map[string]*unstructured.Unstructured{
		"nil":      nil,
		"no body":  {},
		"nil body": {Object: nil},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, st.ApplyChange(ctx, watch.Added, u))
			require.Error(t, st.ApplyChange(ctx, watch.Deleted, u))
		})
	}
}

// A delete carrying no uid cannot be applied — there is no row to key. Reporting success
// for it told the driver a delta had landed, and the driver books that as progress and lets
// a bookmark advance past it, while the delta's own cookie write never happened. A crash in
// that window resumed the watch from an older position and replayed. The write path already
// rejects an empty uid; the delete path now agrees.
func TestDeleteWithoutAUIDIsAnError(t *testing.T) {
	st := newObjectStore(openTestCache(t), deploymentsKind)
	u := newDeployment("", "nameless")
	require.Error(t, st.ApplyChange(context.Background(), watch.Deleted, u))
}
