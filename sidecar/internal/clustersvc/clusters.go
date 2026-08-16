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
// record served to resolvers, its delta-watch frame, the Clusters implementation,
// its controller, and the importers that create the records. Mirrors the Cluster
// section of graph/schema.graphqls.
package clustersvc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// ClusterGroupKind identifies the Cluster beehive resource kind.
var ClusterGroupKind = beehive.GroupKind{Kind: "Cluster"}

// Names are per-kind reconcile/uniqueness keys, NOT identities (identity is the
// beehive ObjectID); each source prefixes its own namespace ("kubeconfig/", future
// "cloud/"). Nothing reads a Cluster back by name.
// See docs/adr/2026-08-09-beehive-control-plane.md.
const namePrefixKubeconfig = "kubeconfig/"

// KubeconfigName returns a kubeconfig-sourced Cluster's beehive name — the
// importer's natural key for one kube-context, not an identity (see ClusterID).
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
		frame := ClusterWatchFrame{Type: clusterFrameType(change), Cluster: toCluster(change.Object)}
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

// clusterFrameType classifies a change that is not a removal. The soft-delete mark is
// one of them: the row is still there, wearing a tombstone the record carries through,
// so it is a Modified like any other field moving.
func clusterFrameType(change beehive.ObjectChange[ClusterSpec, ClusterStatus]) DeltaFrameType {
	if change.Type == beehive.Added {
		return DeltaFrameAdded
	}
	return DeltaFrameModified
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

// Delete requests deletion; beehive's GC collects the record and cascades to what it
// owns. A record already gone is the outcome the caller asked for, so a missing one is
// not an error. A context still present in the kubeconfig is re-imported under a fresh
// id — see TODO.md for the trigger that has to reach the importer for that to be prompt.
func (a clustersAPI) Delete(ctx context.Context, id ClusterID) error {
	if err := a.s.clusterClient.Delete(ctx, beehive.ObjectID(id)); err != nil && !errors.Is(err, beehive.ErrNotFound) {
		return fmt.Errorf("delete cluster %d: %w", id, err)
	}

	// Deleting a record whose context is still in the file changes nothing the importer
	// watches, so without this the context is not re-imported until the user next edits
	// the kubeconfig. The name is held until the row drains, so this pass fails and the
	// importer's retry ladder covers the tail.
	a.s.clusterCtrl.resyncImports()
	return nil
}

// kubeconfigUnreadRequeue paces a reconcile that arrived before the watcher's first
// read. The read is synchronous in the watcher's Start, so this is only ever waited
// out by the window between beehive starting and the controllers starting.
const kubeconfigUnreadRequeue = time.Second

// clusterController reconciles a tracked cluster: today it observes what the
// kubeconfig says about the record's context. Resolving credentials, probing the API
// server, and owning the ClusterCache for the identity it finds are still to come.
//
// It owns the Cluster kind's machinery too, so per-kind detail stays out of the
// service struct: the kubeconfig watcher, which Reconcile reads to observe a
// context's presence, and the importer that creates the records it reconciles and
// wakes them when the file changes. Reconcile never touches the importer.
type clusterController struct {
	kubeconfigWatcher  *kubeconfig.Watcher
	kubeconfigImporter *kubeconfigImporter
}

func newClusterController(kubeconfigPath string, pokeSvc *poke.Service, clusterClient beehive.Client[ClusterSpec, ClusterStatus]) *clusterController {
	watcher := kubeconfig.New(kubeconfigPath, pokeSvc)
	return &clusterController{
		kubeconfigWatcher:  watcher,
		kubeconfigImporter: newKubeconfigImporter(watcher, clusterClient),
	}
}

// Start launches the kind's background work. Watcher before importer: the importer
// subscribes current-on-subscribe, so it must have something to subscribe to.
func (c *clusterController) Start(ctx context.Context) (func(context.Context) error, error) {
	return startAll(ctx, c.machinery())
}

// Close releases what the stop func left, in the same reverse order.
func (c *clusterController) Close() error {
	return closeAll(c.machinery())
}

// machinery lists the kind's leaves in start order.
func (c *clusterController) machinery() []startCloser {
	return []startCloser{c.kubeconfigWatcher, c.kubeconfigImporter}
}

// resyncImports asks the importer for a pass; see kubeconfigImporter.Resync.
func (c *clusterController) resyncImports() { c.kubeconfigImporter.Resync() }

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
	// record left unsettled by a previous process before the watcher's first read.
	// Observing the pre-read config would report every present context absent and wake
	// the kind's watches for a flap. The importer's first pass wakes every record once
	// the watcher loads; the requeue is the backstop for a pass that failed.
	cfg, loaded := c.kubeconfigWatcher.Get()
	if !loaded {
		return beehive.Result{RequeueAfter: kubeconfigUnreadRequeue}, nil
	}

	status := clusterStatus(obj)
	prev := status.Source.Kubeconfig
	status.Source.Kubeconfig = observeKubeconfig(cfg, obj.Spec.Source.Kubeconfig, prev)

	// The importer wakes every record on every kubeconfig snapshot, so most passes
	// observe nothing new, and UpdateStatus would marshal the status and open a
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

// generationSettled reports whether the generation this reconcile was handed is already
// recorded, which is beehive's own gate for dropping a repeat report.
func generationSettled(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	return obj.ObservedGeneration != nil && *obj.ObservedGeneration >= obj.Generation
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

// --- Importers ---
//
// An importer keeps the Cluster records for one ClusterSpecSource variant in step
// with that source; today only the kubeconfig, with cloud accounts joining as a peer.
// Manual creation has no importer — a mutation creates the record and a mutation
// removes it — so "every Cluster has an importer behind it" is not an invariant to
// lean on.
//
// An importer runs outside beehive: it decides which objects exist, which means
// running when there are none, and a controller reconciles one object that already
// does.

// kubeconfigSource is the kubeconfig watcher as its subscribers see it, narrow so a
// test can substitute a hand-driven one. Reconcile reads the watcher directly instead.
type kubeconfigSource interface {
	Subscribe() kubeconfig.Subscription
}

// A failed pass is retried on a doubling delay from importRetryBase, capped at
// importRetryMax. Nothing else re-runs it: the loop is driven by kubeconfig CHANGES,
// so "the next snapshot fixes it" can mean never. The cap is what keeps a lasting
// failure — a store that stays unwritable, a name held by a record that never drains —
// re-leveling at a sane cadence instead of every couple of seconds forever.
const (
	importRetryBase = 2 * time.Second
	importRetryMax  = 5 * time.Minute
)

// kubeconfigImporter keeps the Cluster records in step with the kubeconfig. Each pass
// creates a record for every context none yet references, then wakes every live
// record's reconcile so it re-observes the snapshot.
//
// It is CREATION-ONLY: it writes nothing but the source reference on a new record,
// and never updates, orphans, or deletes. A departed context is orphaned by
// clusterController observing it absent (IsPresent=false), and a returning one reuses
// its never-deleted record. The wake is not an exception — a requeue asks for a
// reconcile, it does not write.
//
// The deterministic name KubeconfigName(context) is the reconcile key, so beehive's
// name-uniqueness rules out a duplicate under a concurrent create.
type kubeconfigImporter struct {
	cfgSource     kubeconfigSource
	clusterClient beehive.Client[ClusterSpec, ClusterStatus]

	// Build seams, defaulted by newKubeconfigImporter and overridden only by this
	// package's tests: the pass itself, and the cadence a test must not outwait.
	sync      func(ctx context.Context, cfg *api.Config) error
	retryBase time.Duration
	retryMax  time.Duration

	// resync requests a pass with no new snapshot behind it. Buffered by one and sent
	// without blocking: the request is only ever "run again", so a second while one is
	// pending adds nothing.
	resync chan struct{}

	wg sync.WaitGroup
}

func newKubeconfigImporter(cfgSource kubeconfigSource, clusterClient beehive.Client[ClusterSpec, ClusterStatus]) *kubeconfigImporter {
	im := &kubeconfigImporter{
		cfgSource:     cfgSource,
		clusterClient: clusterClient,
		retryBase:     importRetryBase,
		retryMax:      importRetryMax,
		resync:        make(chan struct{}, 1),
	}
	im.sync = im.syncClusterSet
	return im
}

// Start launches the loop and returns the func that ends it. The subscription is
// established before Start returns and is current-on-subscribe, so startup state is
// imported immediately. Nothing here can fail.
func (im *kubeconfigImporter) Start(context.Context) (func(context.Context) error, error) {
	// Not Start's context, which bounds startup: this one bounds the loop and the pass
	// in flight, so it lives until the stop func cancels it.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	sub := im.cfgSource.Subscribe()
	im.wg.Go(func() {
		defer sub.Close()
		im.run(loopCtx, sub)
	})

	return func(ctx context.Context) error {
		stopLoop()
		return drain.WithContext(ctx, im.wg.Wait)
	}, nil
}

// Resync asks for a pass against the last snapshot. The loop's only other trigger is a
// kubeconfig change, so whatever frees a context out of band — a Cluster deleted while
// its context is still in the file — has to say so, or nothing re-imports it.
func (im *kubeconfigImporter) Resync() {
	select {
	case im.resync <- struct{}{}:
	default:
	}
}

// Close is a no-op: the loop releases its subscription as it exits.
func (im *kubeconfigImporter) Close() error { return nil }

// nextRetryDelay returns the delay to follow one of d: doubled, then held at max.
func nextRetryDelay(d, max time.Duration) time.Duration {
	return min(2*d, max)
}

// run passes each kubeconfig snapshot through sync until stopped or the watcher
// closes, retrying a failed pass against the snapshot that failed.
func (im *kubeconfigImporter) run(ctx context.Context, sub kubeconfig.Subscription) {
	// Kept so the retry has a snapshot to run against: it fires without one arriving.
	var last *api.Config

	delay := im.retryBase
	retry := time.NewTimer(delay)
	retry.Stop()
	defer retry.Stop()

	attempt := func() {
		err := im.sync(ctx, last)
		switch {
		case err == nil:
			// A newer snapshot can arrive and pass cleanly while an earlier failure's
			// retry is still armed. Leaving it armed fires it against work already done.
			retry.Stop()
			delay = im.retryBase
		case ctx.Err() != nil:
			// The store call was cancelled with the loop, so the error describes the
			// shutdown, not the pass. Nothing to retry: the next select exits.
		default:
			slog.Error("kubeconfig sync failed", "retryIn", delay, "err", err)
			retry.Reset(delay)
			delay = nextRetryDelay(delay, im.retryMax)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case cfg, ok := <-sub.Chan():
			if !ok {
				return
			}
			last = cfg
			attempt()
		case <-retry.C:
			// last is always set here: the timer is armed only inside attempt.
			attempt()
		case <-im.resync:
			// Nothing to run against before the first snapshot, which is on its way.
			if last != nil {
				attempt()
			}
		}
	}
}

// syncClusterSet creates a Cluster for every context in cfg that no record references
// yet, then requeues the records that already existed — one pass, because both need
// the same trigger and the same List. Safe to call any number of times: an unchanged
// snapshot creates nothing, and a requeue whose reconcile observes no change writes
// nothing.
//
// No single context aborts the pass. Contexts come out of a map, so stopping at the
// first failure would import an arbitrary subset that differs run to run — the worst
// shape a partial import has.
func (im *kubeconfigImporter) syncClusterSet(ctx context.Context, cfg *api.Config) error {
	objs, err := im.clusterClient.List(ctx)
	if err != nil {
		return fmt.Errorf("listing clusters: %w", err)
	}

	// A record awaiting deletion counts for neither pass: its context is unclaimed
	// again and owed a fresh record (creating it fails while the name is still held,
	// which the retry covers), and it owes no observation.
	live := make([]*beehive.Object[ClusterSpec, ClusterStatus], 0, len(objs))
	imported := make(map[string]struct{}, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		live = append(live, obj)
		if src := obj.Spec.Source.Kubeconfig; src != nil {
			imported[src.Context] = struct{}{}
		}
	}

	var errs []error
	for contextName := range cfg.Contexts {
		if _, ok := imported[contextName]; ok {
			continue
		}
		// Enabled on arrival, both axes: a tracked context the user has to switch on
		// before it appears in the picker reads as a broken import, not a default.
		spec := ClusterSpec{
			Enabled:     true,
			SyncEnabled: true,
			Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: contextName}},
		}
		if _, err := im.clusterClient.Create(ctx, KubeconfigName(contextName), spec); err != nil {
			errs = append(errs, fmt.Errorf("creating cluster for context %q: %w", contextName, err))
		}
	}

	// Only the records that predate this pass: beehive already owes a first reconcile
	// to everything created above.
	errs = append(errs, im.wakeAll(ctx, live))
	return errors.Join(errs...)
}

// wakeAll asks beehive to reconcile each record, which is how a departed context is
// observed absent: Reconcile reads the watcher rather than the object, and beehive
// cannot know that read went stale. A departure is by definition absent from the
// snapshot the create loop walks, so nothing else would reach it.
//
// A lost wake is the failure nothing else re-levels: the watcher republishes only when
// the file's contents differ, so an unchanged backstop tick re-runs no pass, and the
// record keeps a stale observation — a departed context still marked present — until
// the user next edits the file. So a failed wake rides the retry ladder with the
// creates. A record collected since the List is the exception: it owes no observation,
// and no later pass would find it to wake.
func (im *kubeconfigImporter) wakeAll(ctx context.Context, objs []*beehive.Object[ClusterSpec, ClusterStatus]) error {
	var errs []error
	for _, obj := range objs {
		if err := im.clusterClient.Requeue(ctx, obj.ID); err != nil && !errors.Is(err, beehive.ErrNotFound) {
			errs = append(errs, fmt.Errorf("waking cluster %d: %w", obj.ID, err))
		}
	}
	return errors.Join(errs...)
}
