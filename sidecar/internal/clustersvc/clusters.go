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
	"fmt"
	"log/slog"
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

	status := ClusterStatus{}
	if obj.Status != nil {
		status = *obj.Status
	}
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

	wg sync.WaitGroup
}

func newKubeconfigImporter(cfgSource kubeconfigSource, clusterClient beehive.Client[ClusterSpec, ClusterStatus]) *kubeconfigImporter {
	im := &kubeconfigImporter{
		cfgSource:     cfgSource,
		clusterClient: clusterClient,
		retryBase:     importRetryBase,
		retryMax:      importRetryMax,
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
