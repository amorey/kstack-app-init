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
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
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

// ClusterActiveUID returns the last-probed kube-system UID, or "" if never probed. It
// selects which owned ClusterCache is active.
func ClusterActiveUID(obj *beehive.Object[ClusterSpec, ClusterStatus]) string {
	if obj.Status != nil && obj.Status.Server.UID != nil {
		return *obj.Status.Server.UID
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

func (a clustersAPI) Get(ctx context.Context, id ClusterID) (*Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) List(ctx context.Context) ([]*Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) Watch(ctx context.Context, id ClusterID) (*Stream[ClusterWatchFrame], error) {
	panic("not implemented")
}

func (a clustersAPI) WatchList(ctx context.Context) (*Stream[ClusterWatchFrame], error) {
	panic("not implemented")
}

func (a clustersAPI) WatchSchedule(ctx context.Context, id ClusterID) (<-chan Schedule, error) {
	panic("not implemented")
}

func (a clustersAPI) SetEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) SetSyncEnabled(ctx context.Context, id ClusterID, enabled bool) (*Cluster, error) {
	panic("not implemented")
}

func (a clustersAPI) Delete(ctx context.Context, id ClusterID) error {
	panic("not implemented")
}

// clusterController reconciles a tracked cluster: resolve its credentials, probe the
// API server, and own the ClusterCache for the identity it finds. A placeholder that
// reconciles to a no-op.
//
// It owns the Cluster kind's machinery too, so per-kind detail stays out of the
// service struct: the kubeconfig watcher, which Reconcile reads to observe a
// context's presence, and the importer that creates the records it reconciles. The
// importer is here for lifecycle ownership alone — Reconcile never touches it.
type clusterController struct {
	kubeconfigWatcher  *kubeconfig.Watcher
	kubeconfigImporter *kubeconfigImporter
}

func newClusterController(kubeconfigPath string, clusterClient beehive.Client[ClusterSpec, ClusterStatus]) *clusterController {
	watcher := kubeconfig.New(kubeconfigPath)
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

func (c *clusterController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterStatus],
	obj *beehive.Object[ClusterSpec, ClusterStatus],
) (beehive.Result, error) {
	return beehive.Result{}, nil
}

// --- Importers ---
//
// An importer creates the Cluster records for one ClusterSpecSource variant; today
// only the kubeconfig, with cloud accounts joining as a peer. Manual creation has no
// importer — a mutation creates the record and a mutation removes it — so "every
// Cluster has an importer behind it" is not an invariant to lean on.
//
// An importer runs outside beehive: it decides which objects exist, which means
// running when there are none, and a controller reconciles one object that already
// does.

// kubeconfigSource is the importer's view of the kubeconfig watcher, narrow so a
// test can substitute a hand-driven one. Reconcile reads presence off the watcher
// directly, so this stays the importer's half of it.
type kubeconfigSource interface {
	Subscribe() kubeconfig.Subscription
}

// importRetryInterval paces the retry of an incomplete import, sized for its
// driving case: a name held by a Cluster still draining, which clears once its
// workers stop.
const importRetryInterval = 2 * time.Second

// errNameHeld means a draining Cluster still holds a context's name — not a
// failure, just an incomplete import to retry against the same snapshot.
var errNameHeld = errors.New("a context's name is held by a Cluster still being deleted")

// kubeconfigImporter creates one Cluster per kube-context and is CREATION-ONLY:
// each snapshot creates a record for every context none yet references, writing
// only the source reference. It never updates, orphans, or deletes. A departed
// context is orphaned by clusterController observing it absent (IsPresent=false),
// and a returning one reuses its never-deleted record.
//
// The deterministic name KubeconfigName(context) is the reconcile key, so
// beehive's name-uniqueness rules out a duplicate under a concurrent create.
type kubeconfigImporter struct {
	cfgSource     kubeconfigSource
	clusterClient beehive.Client[ClusterSpec, ClusterStatus]

	// Build seams, defaulted by newKubeconfigImporter and overridden only by this
	// package's tests: the import step, and the cadence a test must not outwait.
	reconcile     func(ctx context.Context, cfg *api.Config) error
	retryInterval time.Duration

	wg sync.WaitGroup
}

func newKubeconfigImporter(cfgSource kubeconfigSource, clusterClient beehive.Client[ClusterSpec, ClusterStatus]) *kubeconfigImporter {
	im := &kubeconfigImporter{
		cfgSource:     cfgSource,
		clusterClient: clusterClient,
		retryInterval: importRetryInterval,
	}
	im.reconcile = im.reconcileClusterSet
	return im
}

// Start launches the import loop and returns the func that ends it. The
// subscription is established before Start returns and is current-on-subscribe, so
// startup state imports immediately. Nothing here can fail.
func (im *kubeconfigImporter) Start(context.Context) (func(context.Context) error, error) {
	// Not Start's context, which bounds startup: this one bounds the loop and the
	// import in flight, so it lives until the stop func cancels it.
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

// Close is a no-op: the loop releases its subscription as it exits.
func (im *kubeconfigImporter) Close() error { return nil }

// run consumes kubeconfig snapshots until stopped or the watcher closes. An
// import failure is logged, not fatal: the level-triggered diff re-derives
// everything from the next snapshot.
func (im *kubeconfigImporter) run(ctx context.Context, sub kubeconfig.Subscription) {
	// Kept so a failure can retry the same snapshot: the loop is driven by kubeconfig
	// CHANGES, so "the next snapshot fixes it" can mean never.
	var last *api.Config
	retry := time.NewTimer(im.retryInterval)
	retry.Stop()
	defer retry.Stop()

	attempt := func() {
		err := im.reconcile(ctx, last)
		switch {
		case err == nil:
			// A newer snapshot can arrive and import cleanly while an earlier
			// failure's retry is still armed. Leaving it armed fires it against work
			// already done.
			retry.Stop()
			return
		case errors.Is(err, errNameHeld):
			slog.Debug("kubeconfig import incomplete, retrying", "err", err)
		default:
			slog.Error("kubeconfig import failed", "err", err)
		}
		retry.Reset(im.retryInterval)
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
		}
	}
}

// reconcileClusterSet creates a Cluster for every context in cfg that no record
// references yet. Safe to call any number of times: an unchanged snapshot writes
// nothing and triggers nothing downstream. Not implemented.
func (im *kubeconfigImporter) reconcileClusterSet(ctx context.Context, cfg *api.Config) error {
	return nil
}
