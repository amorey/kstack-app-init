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

// The Cluster kind: a tracked kube-context. Its beehive spec/status shapes, the
// record served to resolvers, its delta-watch frame, the Clusters implementation, and
// its controller. Mirrors the Cluster section of graph/schema.graphqls. The discovery
// pass that creates the records is driven from clustersources.go.
package clustersvc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// ClusterGroupKind identifies the Cluster beehive resource kind.
var ClusterGroupKind = beehive.GroupKind{Kind: "Cluster"}

// Names are per-kind reconcile/uniqueness keys, NOT identities (identity is the
// beehive ObjectID); each source prefixes its own namespace ("kubeconfig/", future
// "cloud/"). Nothing reads a Cluster back by name.
// See docs/adr/2026-08-09-beehive-control-plane.md.
const namePrefixKubeconfig = "kubeconfig/"

// KubeconfigName returns a kubeconfig-sourced Cluster's beehive name — the
// discovery pass's natural key for one kube-context, not an identity (see ClusterID).
func KubeconfigName(contextName string) string {
	return namePrefixKubeconfig + contextName
}

// ClusterStatusSourceKubeconfig is the kubeconfig-sourced record's last-known
// kubeconfig observation: the cluster/user entry names and presence. Cached from the
// last time the context was present, so it survives orphaning.
type ClusterStatusSourceKubeconfig struct {
	Cluster   string `json:"cluster"`
	User      string `json:"user"`
	IsPresent bool   `json:"isPresent"`
	IsDefault bool   `json:"isDefault"`
}

// ClusterSpecSource is the discriminated union naming where a cluster record
// comes from and how its credentials resolve.
type ClusterSpecSource struct {
	Kubeconfig *ClusterSpecSourceKubeconfig `json:"kubeconfig,omitempty"`
}

// ClusterSpecSourceKubeconfig is the kubeconfig-sourced variant of ClusterSpecSource.
type ClusterSpecSourceKubeconfig struct {
	Context string `json:"context"`
}

// ClusterStatusSource is the status-side counterpart of ClusterSpecSource.
type ClusterStatusSource struct {
	Kubeconfig *ClusterStatusSourceKubeconfig `json:"kubeconfig,omitempty"`
}

// ClusterServer holds last-known facts about the remote cluster, discovered by
// connecting. Nil fields mean never probed.
type ClusterServer struct {
	UID     *string `json:"uid,omitempty"`
	Version *string `json:"version,omitempty"`
}

// ClusterPrincipal holds last-known facts about the connecting client's
// identity on the cluster. Nil fields mean never probed.
type ClusterPrincipal struct {
	Username *string `json:"username,omitempty"`
}

// ClusterSpec is a cluster record's desired state (user/API-owned). No trigger
// counters — retries and resync pokes ride out-of-band buses, never spec writes.
type ClusterSpec struct {
	Name        *string `json:"name,omitempty"`
	Enabled     bool    `json:"enabled"`
	SyncEnabled bool    `json:"syncEnabled"`
	// Source says where this record comes from and how credentials resolve; the
	// matching observation lives on ClusterStatus.Source, rewritten each reconcile.
	Source ClusterSpecSource `json:"source"`
}

// ClusterStatus is both the stored status and the one served to GraphQL:
// connection/health observations. Sync status lives on the ClusterCache child, so
// there is no merge type.
type ClusterStatus struct {
	Source          ClusterStatusSource `json:"source"`
	Server          ClusterServer       `json:"server"`
	Principal       ClusterPrincipal    `json:"principal"`
	LastConnectedAt *time.Time          `json:"lastConnectedAt,omitempty"`
}

// clusterStatus returns the stored status, or the zero value: beehive leaves Status
// nil until a controller first writes one.
func clusterStatus(obj *beehive.Object[ClusterSpec, ClusterStatus]) ClusterStatus {
	if obj.Status == nil {
		return ClusterStatus{}
	}
	return *obj.Status
}

// ClusterActiveUID returns the last-probed kube-system UID, or "" if never probed. It
// selects which owned ClusterCache is active.
func ClusterActiveUID(obj *beehive.Object[ClusterSpec, ClusterStatus]) string {
	if uid := clusterStatus(obj).Server.UID; uid != nil {
		return *uid
	}
	return ""
}

// CacheIsActive reports whether a cache mirrors its parent's currently-active
// identity; one for an unknown identity never is. The single definition of "active
// cache" — sync gating and the read-side join must not disagree.
func CacheIsActive(clusterObj *beehive.Object[ClusterSpec, ClusterStatus], cacheUID string) bool {
	active := ClusterActiveUID(clusterObj)
	return active != "" && cacheUID == active
}

// Cluster is the record for one tracked cluster connection (one kube-context), built
// from a single Cluster beehive object. Owned ClusterCache records are not joined in
// here — they stream standalone via Caches().Watch, so cache churn never re-emits a
// cluster. Nor is the next-reconcile time: a scheduling change fires no WatchList, so
// it is a gauge on Clusters().WatchSchedule instead.
type Cluster struct {
	ID                  ClusterID
	Generation          int64
	CreatedAt           time.Time
	DeletionRequestedAt *time.Time // beehive's soft-delete tombstone, surfaced as-is

	Spec   ClusterSpec
	Status ClusterStatus
	// Conditions are beehive object rows, not part of Status — read off the object
	// rather than out of the status blob.
	Conditions []Condition
}

// ClusterWatchFrame is one frame on the cluster list watch: what happened (Type) to
// which cluster (Cluster), or the Bookmark closing the snapshot, which carries no
// cluster. On a Deleted change Cluster holds the last-known state; consumers key on
// Cluster.ID. Binds 1:1 to the GraphQL ClusterWatchFrame.
type ClusterWatchFrame struct {
	Type    DeltaFrameType
	Cluster *Cluster
}

// toCluster builds the served record from the stored object. Conditions come off the
// object rows rather than the status blob, which is where beehive keeps them.
func toCluster(obj *beehive.Object[ClusterSpec, ClusterStatus]) *Cluster {
	return &Cluster{
		ID:                  ClusterID(obj.ID),
		Generation:          obj.Generation,
		CreatedAt:           obj.CreatedAt,
		DeletionRequestedAt: obj.DeletionRequestedAt,
		Spec:                obj.Spec,
		Status:              clusterStatus(obj),
		Conditions:          obj.Conditions,
	}
}

func (a clustersAPI) Get(ctx context.Context, id ClusterID) (*Cluster, error) {
	obj, err := a.s.clusterClient.Get(ctx, beehive.ObjectID(id))
	if err != nil {
		// A caller holds ids from watch frames, so a record collected in between is an
		// ordinary race rather than a bad request.
		if errors.Is(err, beehive.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get cluster %d: %w", id, err)
	}
	return toCluster(obj), nil
}

// clusterSortKey is the label a list is ordered by: the display name the user set, or
// the kube-context a view renders in its place. The beehive name is the last resort —
// it is a reconcile key rather than anything a caller sees, so it orders only a record
// whose source has no natural name yet.
func clusterSortKey(obj *beehive.Object[ClusterSpec, ClusterStatus]) string {
	if name := obj.Spec.Name; name != nil && *name != "" {
		return *name
	}
	if src := obj.Spec.Source.Kubeconfig; src != nil {
		return src.Context
	}
	return obj.Name
}

func (a clustersAPI) List(ctx context.Context) ([]*Cluster, error) {
	objs, err := a.s.clusterClient.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list clusters: %w", err)
	}

	// The schema promises name order; beehive lists in storage order.
	slices.SortFunc(objs, func(a, b *beehive.Object[ClusterSpec, ClusterStatus]) int {
		return cmp.Or(
			cmp.Compare(clusterSortKey(a), clusterSortKey(b)),
			// Display names are not unique, so a total order needs one that is.
			cmp.Compare(a.Name, b.Name),
		)
	})

	clusters := make([]*Cluster, 0, len(objs))
	for _, obj := range objs {
		clusters = append(clusters, toCluster(obj))
	}
	return clusters, nil
}

func (a clustersAPI) Watch(ctx context.Context, id ClusterID) (*Stream[ClusterWatchFrame], error) {
	src, err := a.s.clusterClient.Watch(ctx, beehive.ObjectID(id))
	if err != nil {
		return nil, fmt.Errorf("watch cluster %d: %w", id, err)
	}

	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterWatchFrame) error {
		// Nil when the id holds nothing yet, which is a snapshot of none rather than a
		// failure: the record may still arrive, and this subscription reports it.
		var snapshot []*beehive.Object[ClusterSpec, ClusterStatus]
		if src.Object != nil {
			snapshot = append(snapshot, src.Object)
		}
		return pumpClusterWatch(ctx, out, snapshot, src.Changes, src.Err)
	}), nil
}

func (a clustersAPI) WatchList(ctx context.Context) (*Stream[ClusterWatchFrame], error) {
	src, err := a.s.clusterClient.WatchList(ctx)
	if err != nil {
		return nil, fmt.Errorf("watch clusters: %w", err)
	}

	return NewStream(ctx, func(ctx context.Context, out chan<- ClusterWatchFrame) error {
		return pumpClusterWatch(ctx, out, src.Objects, src.Changes, src.Err)
	}), nil
}

// pumpClusterWatch streams a snapshot, the bookmark closing it, then every change
// above it. The two cluster watches differ only in what the snapshot holds.
//
// beehive hands the snapshot complete as of a resource version, with Changes carrying
// everything above it, so the bookmark lands between the two without holding any frame
// back. srcErr is the upstream's terminal reason, read once its changes run out.
func pumpClusterWatch(
	ctx context.Context,
	out chan<- ClusterWatchFrame,
	snapshot []*beehive.Object[ClusterSpec, ClusterStatus],
	changes <-chan beehive.ObjectChange[ClusterSpec, ClusterStatus],
	srcErr func() error,
) error {
	for _, obj := range snapshot {
		if !sendFrame(ctx, out, ClusterWatchFrame{Type: DeltaFrameAdded, Cluster: toCluster(obj)}) {
			return nil
		}
	}
	if !sendFrame(ctx, out, ClusterWatchFrame{Type: DeltaFrameBookmark}) {
		return nil
	}

	for change := range changes {
		if change.Type == beehive.Deleted {
			if !sendFrame(ctx, out, clusterDeparture(change)) {
				return nil
			}
			continue
		}

		// Only a removal arrives without an object.
		if change.Object == nil {
			continue
		}
		frame := ClusterWatchFrame{Type: deltaFrameType(change), Cluster: toCluster(change.Object)}
		if !sendFrame(ctx, out, frame) {
			return nil
		}
	}
	return srcErr()
}

// clusterDeparture builds the frame for a removal. beehive reports one whose final row
// it could not decode with no object at all, and the frame still has to carry the id:
// nothing later in the log mentions a deleted id, and a consumer drops a change with no
// entity rather than folding it — so a dropped frame strands the record in its map for
// the life of the subscription.
func clusterDeparture(change beehive.ObjectChange[ClusterSpec, ClusterStatus]) ClusterWatchFrame {
	cluster := &Cluster{ID: ClusterID(change.ID)}
	if change.Object != nil {
		cluster = toCluster(change.Object)
	}
	return ClusterWatchFrame{Type: DeltaFrameDeleted, Cluster: cluster}
}

func (a clustersAPI) WatchSchedule(ctx context.Context, id ClusterID) (<-chan Schedule, error) {
	panic("not implemented")
}

func (a clustersAPI) SetEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error) {
	return a.updateSpec(ctx, id, func(spec *ClusterSpec) { spec.Enabled = enabled })
}

func (a clustersAPI) SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error) {
	return a.updateSpec(ctx, id, func(spec *ClusterSpec) { spec.SyncEnabled = enabled })
}

// updateSpec reads the spec, applies edit, and writes it back. Read-modify-write
// because beehive takes the whole spec: a setter that built one from its argument
// alone would clear every field it does not name.
//
// Serialized because that read and write are not atomic and beehive's Update has no
// compare-and-swap: two setters editing different fields would each write a whole spec
// built from the same read, and the later write would silently restore what the
// earlier one changed. One lock for the kind is enough — the sidecar is beehive's sole
// writer, and these are user-driven.
func (a clustersAPI) updateSpec(ctx context.Context, id ClusterID, edit func(*ClusterSpec)) (*Cluster, error) {
	a.s.clusterSpecMu.Lock()
	defer a.s.clusterSpecMu.Unlock()

	obj, err := a.s.clusterClient.Get(ctx, beehive.ObjectID(id))
	if err != nil {
		return nil, wrapClusterErr("get", id, err)
	}
	spec := obj.Spec
	edit(&spec)
	// The record can be collected between the read and the write, so this reports a
	// missing record too.
	updated, err := a.s.clusterClient.Update(ctx, beehive.ObjectID(id), spec)
	if err != nil {
		return nil, wrapClusterErr("update", id, err)
	}
	return toCluster(updated), nil
}

// wrapClusterErr annotates a store error, mapping beehive's missing-record sentinel
// onto the boundary's own: a caller matches clustersvc.ErrNotFound without having to
// know which store is underneath.
func wrapClusterErr(verb string, id ClusterID, err error) error {
	if errors.Is(err, beehive.ErrNotFound) {
		return fmt.Errorf("%s cluster %d: %w", verb, id, ErrNotFound)
	}
	return fmt.Errorf("%s cluster %d: %w", verb, id, err)
}

// Delete forgets a record its source no longer declares: beehive's GC collects it and
// cascades to the cache it owns. A record already gone is the outcome the caller asked
// for, so a missing one is not an error.
//
// A record its source still declares is refused with ErrDeclaredBySource rather than
// deleted, because the discovery pass would re-import it under a fresh id — the delete
// would read as succeeding and then undo itself.
func (a clustersAPI) Delete(ctx context.Context, id ClusterID) error {
	obj, err := a.s.clusterClient.Get(ctx, beehive.ObjectID(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read cluster %d: %w", id, err)
	}
	if sourceDeclares(a.s.kubeconfigSvc, obj) {
		return fmt.Errorf("delete cluster %d: %w", id, ErrDeclaredBySource)
	}

	if err := a.s.clusterClient.Delete(ctx, beehive.ObjectID(id)); err != nil && !errors.Is(err, beehive.ErrNotFound) {
		return fmt.Errorf("delete cluster %d: %w", id, err)
	}
	return nil
}

// sourceDeclares reports whether the record's source still lists it. A record from no
// source is nobody's to declare.
//
// The kubeconfig itself is the answer whenever it has been read — the record's own
// observation is a cached view of that file, and it is nil for exactly as long as a
// just-imported record has not reconciled yet, which is the window a delete must not
// slip through. The stored observation is the fallback while the file is unread, and a
// record with neither is refused: refusing is recoverable, since the caller retries a
// moment later, where allowing is not — the discovery pass re-imports the context under
// a fresh id and the user's toggles are gone with the old one.
func sourceDeclares(cfgSvc kubeconfigService, obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	src := obj.Spec.Source.Kubeconfig
	if src == nil {
		return false
	}
	if cfg, loaded := cfgSvc.Get(); loaded {
		_, ok := cfg.Contexts[src.Context]
		return ok
	}
	if observed := clusterStatus(obj).Source.Kubeconfig; observed != nil {
		return observed.IsPresent
	}
	return true
}

// startupRequeue paces a reconcile that arrived inside a startup ordering window:
// before the kubeconfig's first read, or before the discovery anchors exist. beehive
// heads service.parts and dispatches its startup pass asynchronously, so a record
// stored by a previous process can reach a reconcile before either has happened. Both
// windows are bounded by the app's own startup, never by anything a user does.
const startupRequeue = time.Second

// clusterController reconciles a tracked cluster: today it observes what the
// kubeconfig says about the record's context, and creates the ClusterCache for the
// identity a probe has recorded. Resolving credentials and probing the API server are
// still to come.
//
// The kubeconfig service it reads to observe a context's presence is the app's,
// shared with every other reader, so it is a dependency rather than machinery.
type clusterController struct {
	lifecycle.None

	// Every kind's client, not just this one's: a cluster creates the ClusterCache
	// children it owns.
	deps
}

func (c *clusterController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterStatus],
	obj *beehive.Object[ClusterSpec, ClusterStatus],
) (beehive.Result, error) {
	// Nothing to observe for a record on its way out, and no finalizer to clear:
	// beehive collects it either way.
	if obj.DeletionRequestedAt != nil {
		return beehive.Result{}, nil
	}

	// beehive starts ahead of the controllers, so its first owed pass can reach a
	// record left unsettled by a previous process before the kubeconfig's first read.
	// Observing the pre-read config would report every present context absent and wake
	// the kind's watches for a flap. The requeue is the backstop until the read lands.
	cfg, loaded := c.kubeconfigSvc.Get()
	if !loaded {
		return beehive.Result{RequeueAfter: startupRequeue}, nil
	}

	// The observation below reads the kubeconfig service rather than this object, so
	// beehive cannot know when it goes stale. The edge onto the source anchor is what
	// turns one status write there into a pass here; without it a departed context
	// stays marked present until something unrelated wakes the record. Beehive records
	// nothing when the edge is already there, so every later pass is free.
	if obj.Spec.Source.Kubeconfig != nil {
		src, err := c.sourceClient.GetByName(ctx, ClusterSourceNameKubeconfig)
		if errors.Is(err, beehive.ErrNotFound) {
			// Not a failure: the bootstrap creates the anchors after beehive starts, and
			// this record predates the process. Failing here would drop every stored
			// record into backoff on every boot, and skip the observation with it.
			return beehive.Result{RequeueAfter: startupRequeue}, nil
		}
		if err != nil {
			return beehive.Result{}, fmt.Errorf("read kubeconfig cluster source: %w", err)
		}
		if err := client.AddDependency(ctx, obj.ID, src.ID); err != nil {
			return beehive.Result{}, fmt.Errorf("depend cluster %d on its source: %w", obj.ID, err)
		}
	}

	// Ahead of the settle fast-path below: a record whose generation is already
	// settled can still be missing the cache its identity calls for, and the pass that
	// returns early there would never create it.
	if err := c.ensureCache(ctx, obj); err != nil {
		return beehive.Result{}, err
	}

	status := clusterStatus(obj)
	prev := status.Source.Kubeconfig
	status.Source.Kubeconfig = observeKubeconfig(cfg, obj.Spec.Source.Kubeconfig, prev)

	// A source's status write wakes every record it declares, so most passes observe
	// nothing new, and UpdateStatus would marshal the status and open a
	// transaction only to find the stored bytes equal. The generation is then all
	// there is left to report — and only while it is unsettled, which a spec write
	// this observation does not depend on (SetEnabled) is what leaves it. An object
	// that stays unsettled is re-dispatched by beehive's owed pass forever.
	if obj.Status != nil && sameKubeconfigObservation(prev, status.Source.Kubeconfig) {
		if generationSettled(obj) {
			return beehive.Result{}, nil
		}
		if err := client.SetObservedGeneration(ctx, obj.ID, obj.Generation); err != nil {
			return beehive.Result{}, fmt.Errorf("record cluster observed generation: %w", err)
		}
		return beehive.Result{}, nil
	}

	if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, status); err != nil {
		return beehive.Result{}, fmt.Errorf("update cluster status: %w", err)
	}
	return beehive.Result{}, nil
}

// ensureCache gives the cluster a mirror slot for the identity its last probe
// recorded. A cluster that has never connected has none to mirror, and a cache named
// for the empty UID is one CacheIsActive matches against nothing.
func (c *clusterController) ensureCache(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterStatus]) error {
	uid := ClusterActiveUID(obj)
	if uid == "" {
		return nil
	}
	return ensureClusterCache(ctx, c.cacheClient, ClusterID(obj.ID), uid)
}

// generationSettled reports whether the generation this reconcile was handed is already
// recorded, which is beehive's own gate for dropping a repeat report.
func generationSettled[Spec, Status any](obj *beehive.Object[Spec, Status]) bool {
	return obj.ObservedGeneration != nil && *obj.ObservedGeneration >= obj.Generation
}

// ensureKubeconfigClusters gives every context in cfg a Cluster record. Called by the
// source's discovery pass; the writes live here so the kind's name and spec stay in
// the kind's file.
//
// CREATION-ONLY: it writes nothing but the source reference on a new record, and
// never updates, orphans, or deletes. A departed context is orphaned by
// clusterController observing it absent (IsPresent=false), and a returning one reuses
// its never-deleted record — which is what preserves the user's toggles across a
// context that comes and goes.
//
// The deterministic name KubeconfigName(context) is the reconcile key, so beehive's
// name-uniqueness rules out a duplicate under a concurrent create.
//
// No single context aborts the pass. Contexts come out of a map, so stopping at the
// first failure would import an arbitrary subset that differs run to run — the worst
// shape a partial import has.
func ensureKubeconfigClusters(ctx context.Context, client beehive.Client[ClusterSpec, ClusterStatus], cfg *api.Config) error {
	objs, err := client.List(ctx)
	if err != nil {
		return fmt.Errorf("listing clusters: %w", err)
	}

	// A record awaiting deletion leaves its context unclaimed and owed a fresh one.
	// Creating it fails while the draining row still holds the name, which is why this
	// pass reports the failure rather than skipping it: beehive's backoff is the retry.
	claimed := make(map[string]struct{}, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		if src := obj.Spec.Source.Kubeconfig; src != nil {
			claimed[src.Context] = struct{}{}
		}
	}

	var errs []error
	for contextName := range cfg.Contexts {
		if _, ok := claimed[contextName]; ok {
			continue
		}
		// Enabled on arrival, both axes: a tracked context the user has to switch on
		// before it appears in the picker reads as a broken import, not a default.
		spec := ClusterSpec{
			Enabled:     true,
			SyncEnabled: true,
			Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: contextName}},
		}
		if _, err := client.Create(ctx, KubeconfigName(contextName), spec); err != nil {
			errs = append(errs, fmt.Errorf("creating cluster for context %q: %w", contextName, err))
		}
	}
	return errors.Join(errs...)
}

// sameKubeconfigObservation compares two observations by value, nil included: a record
// from another source never has one, and two of those are equally unchanged.
func sameKubeconfigObservation(a, b *ClusterStatusSourceKubeconfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// observeKubeconfig returns what cfg says about the record src comes from, folded over
// the previous observation. A nil src is any other source, whose observation is this
// one's to return unchanged.
//
// A departed context keeps its last-known cluster and user names with
// IsPresent=false, which is what keeps an orphaned record identifiable rather than
// blank.
func observeKubeconfig(cfg *api.Config, src *ClusterSpecSourceKubeconfig, prev *ClusterStatusSourceKubeconfig) *ClusterStatusSourceKubeconfig {
	if src == nil {
		return prev
	}

	if kctx := cfg.Contexts[src.Context]; kctx != nil {
		return &ClusterStatusSourceKubeconfig{
			Cluster:   kctx.Cluster,
			User:      kctx.AuthInfo,
			IsPresent: true,
			IsDefault: src.Context == cfg.CurrentContext,
		}
	}

	observed := ClusterStatusSourceKubeconfig{}
	if prev != nil {
		observed = *prev
	}
	observed.IsPresent = false
	observed.IsDefault = false
	return &observed
}
