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

// The ClusterCachedCatalog kind: one discovery anchor per cache, naming the kinds
// that cache's cluster serves. Its beehive shapes, the record served to resolvers,
// its delta-watch frame, the CachedCatalogs implementation, and its controller.
// Mirrors the ClusterCachedCatalog section of graph/schema.graphqls.
package clustersvc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/amorey/beehive"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// ClusterCachedCatalogGroupKind identifies the discovery anchor kind: one
// object per ClusterCache, owned by it; its controller maintains one
// ClusterCachedResource child per served GVR.
var ClusterCachedCatalogGroupKind = beehive.GroupKind{Kind: "ClusterCachedCatalog"}

// ClusterCachedCatalogName returns "cachedcatalog/{cacheObjID}" — exactly one
// per cache, so creation is idempotent under name-uniqueness dedup. A
// creation/dedup key only; the child is enumerated through the owner edge.
func ClusterCachedCatalogName(cacheID beehive.ObjectID) string {
	return "cachedcatalog/" + strconv.FormatInt(int64(cacheID), 10)
}

// ClusterCachedCatalogSpec is the desired discovery for one cache. Enabled is
// the pause switch, evaluated once above and relayed into each child. Existence
// means "has an anchor", NOT "is discovering" — the object lives as long as the
// cache, so its subtree survives a pause.
type ClusterCachedCatalogSpec struct {
	Enabled bool `json:"enabled"`
}

// ClusterCachedCatalogStatus is empty, deliberately: status is a propagation
// channel — for state a dependent reacts to, not for a discovery pass's gauges,
// which nothing in the object graph reads. The Discovered condition is what remains.
type ClusterCachedCatalogStatus struct{}

// ClusterCachedCatalog is the view of one ClusterCachedCatalog beehive
// object: a cache's kind-catalog record — which kinds the cluster serves, when that
// was last confirmed, and whether the confirmation was complete. Streamed standalone
// via CachedCatalogs().Watch and joined onto its cache client-side by Owner.ID. Spec is the
// stored value served as-is, no projection.
type ClusterCachedCatalog struct {
	ID ClusterCachedCatalogID
	// Owner is the ClusterCache this catalog belongs to.
	Owner ObjectRef
	Spec  ClusterCachedCatalogSpec
	// Conditions are beehive object conditions, read off the object rather than out of
	// the status blob — `Discovered`, carrying this component's own verdict. There is no
	// Status field: the kind's status is empty by design.
	Conditions []Condition
}

// ClusterCachedCatalogWatchFrame is one frame on the GVR-discovery watch, the third of
// the parallel object streams (clusters, caches, gvr-discoveries). Binds
// 1:1 to the GraphQL ClusterCachedCatalogWatchFrame; consumers key on Catalog.ID.
type ClusterCachedCatalogWatchFrame struct {
	Type    DeltaFrameType
	Catalog *ClusterCachedCatalog
}

// ensureClusterCachedCatalog gives one cache its discovery anchor, owned by the cache
// so beehive's GC cascades to it, and converges the pause switch onto it. Idempotent:
// the name is the dedup key, and a spec already in the desired state writes nothing.
//
// Called by the cache's reconcile, which is where the pause switch is evaluated; the
// writes live here so the kind's vocabulary stays in the kind's file.
func ensureClusterCachedCatalog(ctx context.Context, client beehive.Client[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus], cacheID ClusterCacheID, enabled bool) error {
	name := ClusterCachedCatalogName(beehive.ObjectID(cacheID))
	spec := ClusterCachedCatalogSpec{Enabled: enabled}

	// One transaction resolves the name and writes; a row awaiting collection is refused
	// rather than rewritten, and its replacement waits for GC to release the name. Same
	// shape as ensureClusterCache's relay.
	_, _, err := client.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(beehive.ObjectID(cacheID)))
	if err != nil && !errors.Is(err, beehive.ErrDeletionPending) {
		return fmt.Errorf("apply cached catalog %s: %w", name, err)
	}
	return nil
}

// toClusterCachedCatalog builds the served record from the stored object.
func toClusterCachedCatalog(obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) (*ClusterCachedCatalog, error) {
	owner, err := toOwnerRef(obj)
	if err != nil {
		return nil, err
	}
	return &ClusterCachedCatalog{
		ID:         ClusterCachedCatalogID(obj.ID),
		Owner:      owner,
		Spec:       obj.Spec,
		Conditions: obj.Conditions,
	}, nil
}

// toClusterCachedCatalogs projects a whole read. beehive lists by id, which is creation
// order, and that is the order this family promises — so nothing here sorts.
func toClusterCachedCatalogs(objs []*beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) ([]*ClusterCachedCatalog, error) {
	catalogs := make([]*ClusterCachedCatalog, 0, len(objs))
	for _, obj := range objs {
		catalog, err := toClusterCachedCatalog(obj)
		if err != nil {
			return nil, err
		}
		catalogs = append(catalogs, catalog)
	}
	return catalogs, nil
}

func (a cachedCatalogsAPI) Get(ctx context.Context, id ClusterCachedCatalogID) (*ClusterCachedCatalog, error) {
	obj, err := a.s.catalogClient.Get(ctx, beehive.ObjectID(id), beehive.LoadOwner())
	if err != nil {
		// A caller holds ids from watch frames, so a record collected in between is an
		// ordinary race rather than a bad request.
		if errors.Is(err, beehive.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get cached catalog %d: %w", id, err)
	}
	return toClusterCachedCatalog(obj)
}

func (a cachedCatalogsAPI) List(ctx context.Context) ([]*ClusterCachedCatalog, error) {
	objs, err := a.s.catalogClient.List(ctx, beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cached catalogs: %w", err)
	}
	return toClusterCachedCatalogs(objs)
}

func (a cachedCatalogsAPI) Watch(ctx context.Context, id ClusterCachedCatalogID) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	src, err := a.s.catalogClient.Watch(ctx, beehive.ObjectID(id), loadCatalogOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached catalog %d: %w", id, err)
	}

	return catalogWatch.streamOne(ctx, src), nil
}

func (a cachedCatalogsAPI) WatchList(ctx context.Context) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	src, err := a.s.catalogClient.WatchList(ctx, loadCatalogOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cached catalogs: %w", err)
	}

	return catalogWatch.streamList(ctx, src), nil
}

func (a cachedCatalogsAPI) ListByCache(ctx context.Context, cacheID ClusterCacheID) ([]*ClusterCachedCatalog, error) {
	// One query rather than a Get on the derived name: the owner edge is what the
	// record is enumerated through, and a cache that has not reconciled yet owns none,
	// which reads empty rather than failing.
	objs, err := a.s.catalogClient.ListOwnedObjects(ctx, beehive.ObjectID(cacheID), beehive.LoadOwner())
	if err != nil {
		return nil, fmt.Errorf("list cache %d cached catalogs: %w", cacheID, err)
	}
	return toClusterCachedCatalogs(objs)
}

func (a cachedCatalogsAPI) WatchByCache(ctx context.Context, cacheID ClusterCacheID) (*Stream[ClusterCachedCatalogWatchFrame], error) {
	src, err := a.s.catalogClient.WatchOwnedObjects(ctx, beehive.ObjectID(cacheID), loadCatalogOwner)
	if err != nil {
		return nil, fmt.Errorf("watch cache %d cached catalogs: %w", cacheID, err)
	}

	return catalogWatch.streamList(ctx, src), nil
}

// loadCatalogOwner eager-loads the owner edge every catalog frame carries as its join
// key; beehive batches the lookup per change batch, so a watch does not become an N+1.
var loadCatalogOwner = beehive.WithLoads(beehive.LoadOwner())

// catalogWatch projects this kind into delta frames. The departure carries the spec but
// no owner: the row is gone, so beehive loads no edge for it and reading one would fail
// the whole stream — and a consumer keys the record it is dropping by id anyway.
var catalogWatch = deltaWatch[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus, ClusterCachedCatalogWatchFrame]{
	frame: func(t DeltaFrameType, obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) (ClusterCachedCatalogWatchFrame, error) {
		catalog, err := toClusterCachedCatalog(obj)
		if err != nil {
			return ClusterCachedCatalogWatchFrame{}, err
		}
		return ClusterCachedCatalogWatchFrame{Type: t, Catalog: catalog}, nil
	},
	departed: func(change beehive.ObjectChange[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus]) ClusterCachedCatalogWatchFrame {
		catalog := &ClusterCachedCatalog{ID: ClusterCachedCatalogID(change.ID)}
		if obj := change.Object; obj != nil {
			catalog.Spec = obj.Spec
			catalog.Conditions = obj.Conditions
		}
		return ClusterCachedCatalogWatchFrame{Type: DeltaFrameDeleted, Catalog: catalog}
	},
	bookmark: ClusterCachedCatalogWatchFrame{Type: DeltaFrameBookmark},
}

// catalogDiscoveryInterval paces re-discovery, registered as the kind's individual pass.
// The third kind whose correctness rests on a poll: what it enumerates is a remote
// server's, so a CRD installed on the cluster moves nothing in the store.
const catalogDiscoveryInterval = 10 * time.Minute

// catalogRetryInterval is how soon a pass comes back when it never reached the server —
// shorter than the cadence, because a cluster that just reconnected should not wait out a
// full interval to learn what it serves. Flat rather than a backoff ladder: these passes
// cost no request at all, since Lease.Conn never dials and a paused catalog never looks.
const catalogRetryInterval = 30 * time.Second

// catalogConcurrency is how many catalogs may be swept at once. A sweep is dozens of
// round-trips bounded by the discovery client's timeout, and beehive's default is one
// worker per controller — so without this the fleet discovers strictly serially and one
// slow API server delays every other cluster's kinds. The same bound, for the same reason,
// that the connection probes run under.
const catalogConcurrency = 8

// clusterCachedCatalogController reconciles one cache's kind catalog: enumerate what the
// cluster serves and maintain a ClusterCachedResource child per kind.
type clusterCachedCatalogController struct {
	lifecycle.None
	// Every kind's client, not just this one's: a catalog reads the cache and cluster it
	// hangs off and writes the per-kind children it owns.
	deps

	// discover is the seam a test substitutes for the API server. Production wires
	// discoverServedKinds; nothing else may be nil here.
	discover func(*kubeconn.Connection) (kindCatalog, error)
}

// kindCatalog is one sweep's outcome: the kinds the server named, and whether that list is
// the whole truth. Partial is what stops the pass pruning — a group that failed to answer
// has not stopped being served, and deleting its children would tear down live workers
// over a momentarily-down aggregated API.
type kindCatalog struct {
	kinds   []ClusterCachedResourceSpec
	partial bool
}

func (c *clusterCachedCatalogController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) beehive.ReconcileResult {
	// A catalog on its way out is about to be collected with the children it owns, and
	// beehive collects it with no finalizer to clear.
	if obj.DeletionRequestedAt != nil {
		return beehive.Settled()
	}

	clusterObj, err := c.clusterOf(ctx, client)
	if err != nil {
		return beehive.Fail(err)
	}
	// The subtree above is being collected, which will take this catalog with it.
	if clusterObj == nil {
		return beehive.Settled()
	}

	// A paused catalog keeps its children and stops discovering: the anchor lives as long
	// as the cache, so the subtree survives a pause and is not rebuilt on resume. The
	// relay still runs, since the switch reaching the workers is what pausing means.
	if !obj.Spec.Enabled {
		return c.relayPause(ctx, client, obj)
	}

	conn, release, err := c.connect(ctx, clusterObj)
	if err != nil {
		// Not a failure to retry under backoff: the cluster is disabled or unreachable,
		// and either is the cluster pass's to report on its own conditions.
		return observeDiscovered(ctx, client, ConditionFalse, ReasonNoConnection, err.Error()).
			RequeueAfter(catalogRetryInterval)
	}
	defer release()

	found, err := c.discover(conn)
	if err != nil {
		// The existing children are left alone: nothing is known about the served kinds
		// this pass, and an empty answer is not the same as "serves nothing".
		//
		// Beehive's ladder rather than the flat retry above, because this one reached the
		// server and a sweep is dozens of round-trips: a cluster answering slowly and
		// failing would otherwise re-sweep every 30s forever, on a worker its whole kind
		// shares. The claim above is released either way — Fail is a return like any other.
		if condErr := observeDiscoveredErr(ctx, client, ConditionFalse, ReasonDiscoveryFailed, err.Error()); condErr != nil {
			return beehive.Fail(condErr)
		}
		return beehive.Fail(fmt.Errorf("discover served kinds: %w", err))
	}
	return c.converge(ctx, client, obj, found)
}

// clusterOf walks the two owner edges above a catalog — cache, then cluster. A nil cluster
// with no error means something in that chain is gone or going, which is a cascade about to
// take this catalog rather than a failure to retry.
func (c *clusterCachedCatalogController) clusterOf(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
) (*beehive.Object[ClusterSpec, ClusterStatus], error) {
	// The reconcile load carries no edges, so each owner is a lookup rather than a field.
	cacheRef, ok, err := client.GetOwner(ctx)
	if err != nil {
		return nil, fmt.Errorf("read cached catalog owner: %w", err)
	}
	if !ok {
		return nil, nil
	}

	cacheObj, err := c.cacheClient.Get(ctx, cacheRef.ID, beehive.LoadOwner())
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read cluster cache %d: %w", cacheRef.ID, err)
	}
	if cacheObj.DeletionRequestedAt != nil {
		return nil, nil
	}

	clusterRef, ok, err := cacheObj.Owner()
	if err != nil {
		return nil, fmt.Errorf("read cluster cache %d owner: %w", cacheRef.ID, err)
	}
	if !ok {
		return nil, nil
	}

	clusterObj, err := c.clusterClient.Get(ctx, clusterRef.ID)
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read cluster %d: %w", clusterRef.ID, err)
	}
	return clusterObj, nil
}

// connect claims the cluster's context for the length of this pass and hands back the
// connection its probe built. The claim is refcounted alongside the one clusterController
// holds, so a pass costs no dial; releasing it is the caller's, hence the release func.
func (c *clusterCachedCatalogController) connect(ctx context.Context, clusterObj *beehive.Object[ClusterSpec, ClusterStatus]) (*kubeconn.Connection, func(), error) {
	contextName, err := clusterContext(clusterObj)
	if err != nil {
		return nil, nil, err
	}

	lease := c.kubeconnSvc.Acquire(contextName)
	conn, err := lease.Conn(ctx)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	return conn, lease.Release, nil
}

// converge rewrites the per-kind children to match what discovery found, and reports
// the verdict.
func (c *clusterCachedCatalogController) converge(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
	found kindCatalog,
) beehive.ReconcileResult {
	held, err := c.resourceClient.ListOwnedObjects(ctx, obj.ID)
	if err != nil {
		return beehive.Fail(fmt.Errorf("list cached catalog %d resources: %w", obj.ID, err))
	}

	draining, err := c.applyKinds(ctx, obj, found.kinds)
	if err != nil {
		return beehive.Fail(err)
	}

	// Pruning needs a complete answer: a group that did not answer has not stopped being
	// served, and deleting its children would stop live workers over a transient outage.
	if !found.partial {
		if err := c.prune(ctx, held, found.kinds); err != nil {
			return beehive.Fail(err)
		}
	}

	status, reason := ConditionTrue, ReasonDiscovered
	switch {
	case found.partial:
		status, reason = ConditionFalse, ReasonDiscoveryPartial
	case draining:
		status, reason = ConditionFalse, ReasonDiscoveryDraining
	}
	res := observeDiscovered(ctx, client, status, reason, "")
	if draining {
		return res.RequeueAfter(catalogRetryInterval)
	}
	return res
}

// relayPause rewrites each live child's switch and changes nothing else: the pass did
// not look at the server, so the desired set is what is already stored.
func (c *clusterCachedCatalogController) relayPause(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
) beehive.ReconcileResult {
	held, err := c.resourceClient.ListOwnedObjects(ctx, obj.ID)
	if err != nil {
		return beehive.Fail(fmt.Errorf("list cached catalog %d resources: %w", obj.ID, err))
	}
	// Draining cannot arise: every name written here belongs to a live row just listed.
	if _, err := c.applyKinds(ctx, obj, specsOf(held)); err != nil {
		return beehive.Fail(err)
	}
	return observeDiscovered(ctx, client, ConditionFalse, ReasonPaused, "")
}

// applyKinds converges one child per kind, relaying the pause switch into each. draining
// reports a served kind whose name is still held by an earlier prune's tombstone: the
// kind has no live record, which is a state to report and come back to, not a failure.
func (c *clusterCachedCatalogController) applyKinds(
	ctx context.Context,
	obj *beehive.Object[ClusterCachedCatalogSpec, ClusterCachedCatalogStatus],
	kinds []ClusterCachedResourceSpec,
) (draining bool, err error) {
	for _, spec := range kinds {
		spec.Enabled = obj.Spec.Enabled
		name := ClusterCachedResourceName(obj.ID, spec.APIVersion, spec.Resource)
		_, _, err := c.resourceClient.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(obj.ID))
		switch {
		case errors.Is(err, beehive.ErrDeletionPending):
			draining = true
		case err != nil:
			return false, fmt.Errorf("apply cached resource %s: %w", name, err)
		}
	}
	return draining, nil
}

// prune deletes the children for kinds the server no longer serves. Deletion is beehive's
// soft one, so the row lingers holding its name — which is the draining case above, and
// why a kind that comes back is reported rather than silently missing.
func (c *clusterCachedCatalogController) prune(ctx context.Context, held []*beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus], kinds []ClusterCachedResourceSpec) error {
	served := make(map[SyncedKindRef]struct{}, len(kinds))
	for _, spec := range kinds {
		served[SyncedKindRef{APIVersion: spec.APIVersion, Resource: spec.Resource}] = struct{}{}
	}

	for _, obj := range held {
		ref := SyncedKindRef{APIVersion: obj.Spec.APIVersion, Resource: obj.Spec.Resource}
		if _, ok := served[ref]; ok || obj.DeletionRequestedAt != nil {
			continue
		}
		if err := c.resourceClient.Delete(ctx, obj.ID); err != nil && !errors.Is(err, beehive.ErrNotFound) {
			return fmt.Errorf("delete cached resource %d: %w", obj.ID, err)
		}
	}
	return nil
}

// specsOf reads the stored specs back out, for the pass that has no fresh answer.
func specsOf(objs []*beehive.Object[ClusterCachedResourceSpec, ClusterCachedResourceStatus]) []ClusterCachedResourceSpec {
	specs := make([]ClusterCachedResourceSpec, 0, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt == nil {
			specs = append(specs, obj.Spec)
		}
	}
	return specs
}

// observeDiscovered records the verdict and settles. The pass writes no status of its own
// — this kind has none — so the condition and the result are the whole report.
func observeDiscovered(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	status ConditionStatus,
	reason, message string,
) beehive.ReconcileResult {
	if err := observeDiscoveredErr(ctx, client, status, reason, message); err != nil {
		return beehive.Fail(err)
	}
	return beehive.Settled()
}

// observeDiscoveredErr is the write alone, for the caller that has already decided the pass
// failed and only needs the verdict on the record before it says so.
func observeDiscoveredErr(
	ctx context.Context,
	client beehive.ControllerClient[ClusterCachedCatalogStatus],
	status ConditionStatus,
	reason, message string,
) error {
	cond := LiveCondition(ConditionDiscovered, status, reason, message)
	if err := client.SetCondition(ctx, cond); err != nil {
		return fmt.Errorf("set %s condition: %w", ConditionDiscovered, err)
	}
	return nil
}

// discoverServedKinds enumerates the kinds the cluster serves, each at the version the
// server itself prefers.
//
// A partial answer is the routine failure here rather than an exception: an aggregated API
// server that is down fails its own group and no other, and client-go hands back the groups
// that did answer alongside the error naming the ones that did not. The caller decides what
// an incomplete list may do — which is everything except prune.
func discoverServedKinds(conn *kubeconn.Connection) (kindCatalog, error) {
	lists, err := conn.Discovery.ServerPreferredResources()

	var groupErr *discovery.ErrGroupDiscoveryFailed
	if err != nil && !errors.As(err, &groupErr) {
		return kindCatalog{}, err
	}
	return kindCatalog{kinds: servedKinds(lists), partial: groupErr != nil}, nil
}

// servedKinds filters a discovery answer down to what a cache can mirror, sorted so a pass
// is deterministic.
func servedKinds(lists []*metav1.APIResourceList) []ClusterCachedResourceSpec {
	var kinds []ClusterCachedResourceSpec
	for _, list := range lists {
		if list == nil || !mirrorableGroup(list.GroupVersion) {
			continue
		}
		for _, res := range list.APIResources {
			if !mirrorableResource(res) {
				continue
			}
			kinds = append(kinds, ClusterCachedResourceSpec{
				APIVersion: list.GroupVersion,
				Kind:       res.Kind,
				Resource:   res.Name,
				Namespaced: res.Namespaced,
			})
		}
	}

	slices.SortFunc(kinds, func(a, b ClusterCachedResourceSpec) int {
		return cmp.Or(cmp.Compare(a.APIVersion, b.APIVersion), cmp.Compare(a.Resource, b.Resource))
	})
	return kinds
}

// mirrorableGroup drops the alternate events spelling. One event store is served under two
// group-versions, so mirroring both would cache every event twice; canonical v1 wins.
func mirrorableGroup(groupVersion string) bool {
	gv, err := schema.ParseGroupVersion(groupVersion)
	return err == nil && gv.Group != EventsAltGroup
}

// mirrorableResource reports whether one resource can back a cache. A subresource has no
// collection of its own, and a kind that cannot be listed and watched cannot be mirrored —
// which is the whole of what a worker does.
func mirrorableResource(res metav1.APIResource) bool {
	if strings.Contains(res.Name, "/") {
		return false
	}
	return slices.Contains(res.Verbs, "list") && slices.Contains(res.Verbs, "watch")
}
