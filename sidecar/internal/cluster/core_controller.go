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

package cluster

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"
	"github.com/amorey/gochan"
	"github.com/amorey/gochan/broadcast"

	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

const (
	// connectionMaxBackoff caps beehive's exponential reconnect backoff for a
	// failing cluster — passed via WithMaxRetryInterval at registration. The base
	// (1s) and ×2 factor are beehive's defaults.
	connectionMaxBackoff = 2 * time.Minute

	// connectionProbeTimeout bounds one identity probe's round-trips.
	connectionProbeTimeout = 10 * time.Second

	// healthProbeInterval paces the periodic server-health probe.
	healthProbeInterval = 30 * time.Second
	// healthProbeTimeout bounds one health probe's round-trip.
	healthProbeTimeout = 10 * time.Second
)

// HealthPhase is the health probe's verdict vocabulary, folded into the
// Healthy condition by healthCondition.
type HealthPhase string

const (
	// HealthPhaseUnknown means health cannot be assessed (no live connection).
	HealthPhaseUnknown HealthPhase = "UNKNOWN"
	// HealthPhaseHealthy means the cluster is reachable with no problems.
	HealthPhaseHealthy HealthPhase = "HEALTHY"
	// HealthPhaseDegraded means reachable but reporting failing readiness checks.
	HealthPhaseDegraded HealthPhase = "DEGRADED"
	// HealthPhaseUnreachable means the API server cannot be reached.
	HealthPhaseUnreachable HealthPhase = "UNREACHABLE"
)

// ProbeFunc dials the cluster and returns its server and principal identity.
// Tests inject a fake; production uses probeCluster.
type ProbeFunc func(ctx context.Context, cfg *rest.Config) (ClusterServer, ClusterPrincipal, error)

// CheckFunc probes the API server's own health endpoint.
// Tests inject a fake; production uses checkServerHealth.
type CheckFunc func(ctx context.Context, cfg *rest.Config) (HealthPhase, *string)

// ClusterCoreController reconciles Cluster beehive objects. On each pass it:
//  1. Creates a ClusterCache child if one does not exist.
//  2. Observes the kubeconfig live and writes the source observation
//     (cluster/user names, presence, isDefault) to ClusterStatus.Source; a
//     departed context keeps its last-known names with IsPresent=false.
//  3. Gates on eligibility (enabled + kubeconfig present + not being deleted),
//     using the freshly-observed presence.
//  4. Resolves REST credentials from the current kubeconfig.
//  5. Probes the connection (server version + UID + principal).
//  6. Probes server health (readyz endpoint).
//  7. Writes all observations to ClusterStatus via UpdateStatus.
//  8. Updates connMgr: Set on success, Delete on failure or ineligibility.
//
// It subscribes to the kubeconfig watcher (StartBackground) and re-reconciles all clusters
// on a change; the status write then wakes each ClusterCache dependent. Reconciles of one
// cluster serialize on its own lock and re-read fresh under it; different clusters run in
// parallel. See docs/adr/2026-08-09-connection-probing.md.
type ClusterCoreController struct {
	cfgSource   KubeConfigSource
	coreClient  beehive.Client[ClusterSpec, ClusterStatus]
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	connMgr     *ConnectionManager
	ctrlClient  beehive.ControllerClient[ClusterStatus]

	// pokeSvc is the resync bus; nil disables poke-driven re-probes (tests).
	pokeSvc *poke.Service

	// Targeted-retry bus: Service.RetryConnection sends an id and the background worker
	// re-probes that one cluster out-of-band. Carries a payload, so it can't ride the
	// payload-less poke bus.
	retryCh chan ClusterID

	// The single background worker: drains the retry bus (one cluster), the poke bus (all),
	// and the kubeconfig watcher (all) in one select.
	bgWG     sync.WaitGroup
	bgCancel context.CancelFunc

	// One long-lived liveness watch per connected cluster, owned by converge. A watch
	// closing is the earliest connection-loss signal, so detection belongs to the connection
	// controller and works whether or not sync runs. See connection_sentinel.go.
	sentinelWatch SentinelWatchFunc
	// Guards the sentinels map and bgCtx (the base context sentinel goroutines derive from).
	sentinelMu sync.Mutex
	sentinels  map[ClusterID]*connSentinel
	sentinelWG sync.WaitGroup
	bgCtx      context.Context

	// Per-cluster reconcile locks. beehive status writes carry no resourceVersion guard, so
	// two reconciles of the SAME cluster must not interleave their read-modify-write. Per
	// cluster and NOT one shared mutex: everything else converge touches is separately
	// guarded, and parallelism is what stops one unreachable cluster's dial timeout from
	// delaying every cluster queued behind it at startup.
	//
	// A cluster's lock is held across its own probe (which also keeps setProbing's true/false
	// pairing correct). Entries are never removed — bounded by contexts ever seen, and
	// freeing a lock another goroutine is about to take is the bug that avoids.
	// See docs/adr/2026-08-09-connection-probing.md.
	reconcileMu    sync.Mutex
	reconcileLocks map[ClusterID]*sync.Mutex

	// Guards the in-flight probe set and serializes hub publishes against a new subscriber's
	// current-value read, so a subscriber neither misses a transition nor double-counts one
	// straddling its subscribe. Drives the webview's definite "checking now".
	probeMu  sync.Mutex
	probing  map[ClusterID]bool
	probeHub *broadcast.Hub[probeUpdate]
	probeTx  *broadcast.Sender[probeUpdate]

	probe ProbeFunc
	check CheckFunc
}

// probeUpdate is one in-flight-probe transition: Active true when a probe starts, false
// when it returns.
type probeUpdate struct {
	ID     ClusterID
	Active bool
}

// Bounds the probe-transition fan-out buffer; at most clusterProbeConcurrency probes run at
// once, each publishing two transitions.
const probeHubCapacity = 16

// Caps concurrent probes — both beehive's worker count for the Cluster kind and
// reconcileAll's pool, since both spend their time in the same probe. Bounded so many
// kube-contexts don't open a TLS handshake each at once; wide enough that a few unreachable
// clusters can't stall the reachable ones.
const clusterProbeConcurrency = 8

// Bounds the targeted-retry bus; a full buffer means a retry is already queued, so further
// Reprobe calls drop (non-blocking).
const retryBufferSize = 64

// Bounds one poke-driven re-probe; the round-trips are separately capped inside probe/check.
const pokeReprobeTimeout = connectionProbeTimeout + healthProbeTimeout

// NewClusterCoreController builds the controller from the shared runtime. rt.connMgr and
// rt.pokeSvc may be nil (no connection tracking / no poke-driven re-probe); probe and check
// default to the real network implementations, tests inject fakes.
func NewClusterCoreController(
	rt *controllerRuntime,
	cfgSource KubeConfigSource,
	probe ProbeFunc,
	check CheckFunc,
) *ClusterCoreController {
	if probe == nil {
		probe = probeCluster
	}
	if check == nil {
		check = checkServerHealth
	}
	probeHub := broadcast.New[probeUpdate](probeHubCapacity)
	return &ClusterCoreController{
		cfgSource:      cfgSource,
		coreClient:     beehive.NewClient[ClusterSpec, ClusterStatus](rt.bh, ClusterGroupKind),
		cacheClient:    beehive.NewClient[ClusterCacheSpec, ClusterCacheStatus](rt.bh, ClusterCacheGroupKind),
		connMgr:        rt.connMgr,
		pokeSvc:        rt.pokeSvc,
		retryCh:        make(chan ClusterID, retryBufferSize),
		probe:          probe,
		check:          check,
		sentinelWatch:  watchKubeSystem,
		sentinels:      make(map[ClusterID]*connSentinel),
		reconcileLocks: make(map[ClusterID]*sync.Mutex),
		probing:        make(map[ClusterID]bool),
		probeHub:       probeHub,
		probeTx:        probeHub.Sender(),
	}
}

// reconcileLock returns id's reconcile lock, creating it on first use. See
// reconcileMu for why the lock is per cluster and never reclaimed.
func (c *ClusterCoreController) reconcileLock(id ClusterID) *sync.Mutex {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	mu, ok := c.reconcileLocks[id]
	if !ok {
		mu = &sync.Mutex{}
		c.reconcileLocks[id] = mu
	}
	return mu
}

// setProbing records (and fans out) whether id's network probe is in flight. The
// map update + publish run under probeMu so a concurrent WatchProbe subscribe (which
// reads the current value and registers under the same lock) sees a consistent
// snapshot with no missed or duplicated transition. tx.Send never blocks.
func (c *ClusterCoreController) setProbing(id ClusterID, active bool) {
	c.probeMu.Lock()
	defer c.probeMu.Unlock()
	if active {
		c.probing[id] = true
	} else {
		delete(c.probing, id)
	}
	_ = c.probeTx.Send(probeUpdate{ID: id, Active: active})
}

// WatchProbe streams whether id's probe is in flight: current-on-subscribe (opening
// mid-probe sees true), then one value per transition; closes when ctx ends. The current
// read and hub subscribe share probeMu, so no transition is lost in the gap. Backs the
// Probing field merged into ClusterScheduleWatch.
func (c *ClusterCoreController) WatchProbe(ctx context.Context, id ClusterID) <-chan bool {
	c.probeMu.Lock()
	cur := c.probing[id]
	rx := c.probeHub.Receiver()
	c.probeMu.Unlock()

	out := make(chan bool, 1)
	go func() {
		defer close(out)
		defer rx.Close()
		// Current-on-subscribe.
		select {
		case out <- cur:
		case <-ctx.Done():
			return
		}
		for {
			u, err := rx.RecvContext(ctx)
			if err == nil {
				if u.ID != id {
					continue
				}
				if !send(ctx, out, u.Active) {
					return
				}
				continue
			}
			var lagged gochan.ErrLagged
			if !errors.As(err, &lagged) {
				return // ctx ended, or the hub closed
			}
			// Re-read the authoritative flag rather than carrying on: the overwritten
			// values may have included this probe FINISHING, and inferring "still probing"
			// from a gap sticks the row on "checking now" until the next probe.
			c.probeMu.Lock()
			cur := c.probing[id]
			c.probeMu.Unlock()
			if !send(ctx, out, cur) {
				return
			}
		}
	}()
	return out
}

// recordAttempt appends one probe outcome to the cluster's event log. beehive coalesces
// consecutive same-(category, type, reason) outcomes into one run, so a repeated failure
// bumps a count instead of writing status — the reason per-probe chatter stays off
// clustersWatch; see docs/adr/2026-08-09-status-propagation-gauges.md. Best-effort.
func (c *ClusterCoreController) recordAttempt(ctx context.Context, client beehive.ControllerClient[ClusterStatus], id beehive.ObjectID, ok bool, reason, message string) {
	typ := beehive.EventWarning
	if ok {
		typ = beehive.EventNormal
	}
	err := client.AddEvent(ctx, id, beehive.EventSpec{
		Category: ConnectionEventCategory,
		Type:     typ,
		Reason:   reason,
		Message:  truncateMessage(message),
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("clustercontroller: record connection event", "cluster", id, "reason", reason, "err", err)
	}
}

// SetControllerClient injects the status-write client from beehive.Register, backing the
// out-of-band reconciles. Call once, before the control plane starts.
func (c *ClusterCoreController) SetControllerClient(cl beehive.ControllerClient[ClusterStatus]) {
	c.ctrlClient = cl
}

// StartBackground launches the single out-of-band worker, draining three sources in one
// select: the targeted-retry bus (one cluster), the resync poke bus, and the kubeconfig
// watcher (both all clusters). A nil pokeSvc leaves that arm dormant. Call after the control
// plane has started; pair with StopBackground.
func (c *ClusterCoreController) StartBackground() {
	ctx, cancelCtx := context.WithCancel(context.Background())

	// Base context the sentinels anchor to. Until it's set ensureSentinel skips (a reconcile
	// may run between bh.Start and here); the next reconcile starts them.
	c.sentinelMu.Lock()
	c.bgCtx = ctx
	c.sentinelMu.Unlock()

	var pokeCh <-chan poke.Signal
	cancelSub := func() {}
	if c.pokeSvc != nil {
		pokeCh, cancelSub = c.pokeSvc.Subscribe()
	}

	// Current-on-subscribe, so the first value reconciles everything at startup.
	kcSub := c.cfgSource.Subscribe()
	c.bgCancel = func() { cancelSub(); kcSub.Close(); cancelCtx() }

	kcCh := kcSub.Chan()
	c.bgWG.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case id := <-c.retryCh:
				c.reprobeOne(ctx, id)
			case _, ok := <-pokeCh:
				if !ok {
					pokeCh = nil // poke bus closed; keep serving retries
					continue
				}
				c.reconcileAll(ctx)
			case _, ok := <-kcCh:
				if !ok {
					kcCh = nil // watcher closed; keep serving retries/pokes
					continue
				}
				c.reconcileAll(ctx)
			}
		}
	})
}

// StopBackground halts the background worker (unsubscribing from the poke bus and
// cancelling any in-flight re-probe) and joins its goroutine. Safe to call when
// StartBackground was never called.
func (c *ClusterCoreController) StopBackground() {
	// Close the sentinel gate before waiting: clear bgCtx under sentinelMu so every
	// subsequent ensureSentinel skips its sentinelWG.Add (it checks bgCtx==nil under
	// this same lock). Beehive reconciles still reach ensureSentinel here — beehive
	// isn't drained until bhStop, which runs after StopBackground — so without the gate
	// a late Add(1) would race the sentinelWG.Wait below (a WaitGroup misuse the race
	// detector flags). The mutex gives every prior Add a happens-before with Wait.
	c.sentinelMu.Lock()
	c.bgCtx = nil
	c.sentinelMu.Unlock()

	if c.bgCancel != nil {
		c.bgCancel() // cancels the worker AND every sentinel (derived from bgCtx)
	}
	c.bgWG.Wait()
	c.sentinelWG.Wait()
}

// Reprobe requests an immediate out-of-band re-probe of one cluster; non-blocking, dropped
// when a retry is already queued. Producers: the connection sentinel and the user-initiated
// retry. Both are backoff-neutral — running off beehive's worker means a failure neither
// resets nor advances the ladder. See docs/adr/2026-08-09-connection-probing.md.
func (c *ClusterCoreController) Reprobe(id ClusterID) {
	select {
	case c.retryCh <- id:
	default:
	}
}

// reconcileAll re-runs the reconcile for every non-deleting cluster, driven by the poke bus
// and the kubeconfig watcher (and so once at startup). The List snapshot only enumerates
// ids; Reconcile re-reads each object fresh, so a just-departed cluster still updates its
// observation and wakes its ClusterCache dependent.
//
// Concurrent, clusterProbeConcurrency at a time: in sequence, one unreachable cluster's dial
// timeout would delay every cluster behind it. Reconcile locks per cluster, so this is safe.
func (c *ClusterCoreController) reconcileAll(baseCtx context.Context) {
	objs, err := c.coreClient.List(baseCtx)
	if err != nil {
		if baseCtx.Err() == nil {
			slog.Warn("clustercontroller: list clusters for re-reconcile", "err", err)
		}
		return
	}
	sem := make(chan struct{}, clusterProbeConcurrency)
	var wg sync.WaitGroup
dispatch:
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-baseCtx.Done():
			break dispatch // shutting down; drain what is already running
		}
		wg.Go(func() {
			defer func() { <-sem }()
			c.reprobeObj(baseCtx, obj)
		})
	}
	wg.Wait()
}

// reprobeOne forces an immediate out-of-band reconcile of one cluster by id. An unknown id
// is a no-op (it may have been deleted before the worker drained the bus).
func (c *ClusterCoreController) reprobeOne(baseCtx context.Context, id ClusterID) {
	obj, err := c.coreClient.Get(baseCtx, beehive.ObjectID(id))
	if err != nil {
		if !errors.Is(err, beehive.ErrNotFound) && baseCtx.Err() == nil {
			slog.Warn("clustercontroller: retry get cluster", "cluster", id, "err", err)
		}
		return
	}
	c.reprobeObj(baseCtx, obj)
}

// reprobeObj forces an out-of-band reconcile of one object. Deliberately no eligibility
// pre-gate: Reconcile decides from freshly re-read state, and a stale pre-gate could skip a
// cluster whose observation was written concurrently.
func (c *ClusterCoreController) reprobeObj(baseCtx context.Context, obj *beehive.Object[ClusterSpec, ClusterStatus]) {
	ctx, cancel := context.WithTimeout(baseCtx, pokeReprobeTimeout)
	defer cancel()
	if _, err := c.Reconcile(ctx, c.ctrlClient, obj); err != nil {
		slog.Warn("clustercontroller: out-of-band re-probe", "cluster", obj.ID, "err", err)
	}
}

// Reconcile converges one Cluster object. The reconcile steps run in sequence;
// the first failure short-circuits (probe failure records an observation and
// requests backoff, store errors return an error for the harness to retry).
func (c *ClusterCoreController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus]) (beehive.Result, error) {
	mu := c.reconcileLock(ClusterID(obj.ID))
	mu.Lock()
	defer mu.Unlock()

	// Re-read fresh under the lock so this reconcile acts on the latest status, not the
	// snapshot it was handed. Status writes have no resourceVersion guard, so a stale
	// snapshot would otherwise clobber a newer source observation.
	fresh, err := c.coreClient.Get(ctx, obj.ID)
	switch {
	case err == nil:
		obj = fresh
	case errors.Is(err, beehive.ErrNotFound):
		return beehive.Result{}, nil
	default:
		// Ends the pass: falling through would converge on the handed snapshot and
		// UpdateStatus it — the exact clobber the lock and re-read exist to prevent.
		return beehive.Result{}, err
	}

	if obj.DeletionRequestedAt != nil {
		// No finalizers to clear; beehive GC handles ClusterCache cascade.
		return beehive.Result{}, nil
	}

	// The ClusterCache child is created post-probe (ensureClusterCache), not here: it
	// is keyed by the kube-system UID, which is unknown until a probe succeeds.

	// Load (or seed) the working status copy.
	var loaded ClusterStatus
	if obj.Status != nil {
		loaded = *obj.Status
	}
	working := ClusterStatus{
		Source:          loaded.Source,
		Server:          loaded.Server,
		Principal:       loaded.Principal,
		LastConnectedAt: loaded.LastConnectedAt,
	}

	var conds conditionSet
	requeueAfter, convergeErr := c.converge(ctx, client, obj, &working, &conds)

	// Persist observations before surfacing any failure — the write must land even when
	// the probe failed. The probe-outcome event was already recorded by converge.
	//
	// One transaction for the whole pass, so a watcher never sees Connected=True beside a
	// stale Server. An unchanged status still settles the generation — conditions don't
	// advance the handshake, and the pass would stay owed forever. See
	// docs/adr/2026-08-09-liveness-conditions.md.
	if err := client.Within(ctx, func(ctx context.Context) error {
		if err := client.SetConditions(ctx, obj.ID, conds); err != nil {
			return err
		}
		if ClusterStatusEqual(loaded, working) {
			return client.SetObservedGeneration(ctx, obj.ID, obj.Generation)
		}
		return client.UpdateStatus(ctx, obj.ID, obj.Generation, working)
	}); err != nil {
		return beehive.Result{}, err
	}
	// Returned as an error so beehive applies its backoff; the status is already recorded.
	if convergeErr != nil {
		return beehive.Result{}, convergeErr
	}
	return beehive.Result{RequeueAfter: requeueAfter}, nil
}

// converge runs the eligibility gate → credential resolution → probe → health
// phases, recording observations on working. It returns (requeueAfter, err): a
// failed probe returns a non-nil error so beehive applies exponential backoff; a
// success returns the steady health-poll interval; an ineligible cluster returns
// (0, nil) — nothing scheduled.
func (c *ClusterCoreController) converge(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus], working *ClusterStatus, conds *conditionSet) (time.Duration, error) {
	clusterID := clusterIDFromObj(obj)

	// The freshly-observed presence drives this reconcile's gate, so it never lags the
	// status about to be written.
	present := c.observeKubeconfig(obj, working)

	if !connectionEligible(obj, present) {
		c.teardownConnection(clusterID)
		conds.set(liveCondition(ConditionConnected, ConditionFalse, ReasonInactive, ""))
		conds.set(liveCondition(ConditionHealthy, ConditionUnknown, ReasonInactive, ""))
		return 0, nil
	}

	// Drives the webview's definite "checking now". The defer clears it on every exit —
	// success, failure, or resolve error alike.
	c.setProbing(clusterID, true)
	defer c.setProbing(clusterID, false)

	// Stamps LastConnectedAt on success; probe outcomes go to the event log separately.
	now := time.Now().UTC()

	contextName := obj.Spec.Source.Kubeconfig.Context
	restCfg, err := ResolveRESTConfig(c.cfgSource.Get(), contextName)
	if err != nil {
		c.teardownConnection(clusterID)
		c.recordAttempt(ctx, client, obj.ID, false, ReasonResolveFailed, err.Error())
		return 0, c.observeConnectFailure(conds, ReasonResolveFailed, err)
	}

	server, principal, err := c.probe(ctx, restCfg)
	if err != nil {
		c.teardownConnection(clusterID)
		c.recordAttempt(ctx, client, obj.ID, false, ReasonProbeFailed, err.Error())
		return 0, c.observeConnectFailure(conds, ReasonProbeFailed, err)
	}

	// Computed once here because this is the only place that sees the kubeconfig's raw
	// proxy-url. The sentinel keys its watch on it; the sync controllers read it back off
	// the ConnectionManager to detect a credential rotation.
	fingerprint := ConfigFingerprint(restCfg, ContextProxyURL(c.cfgSource.Get(), contextName))
	if c.connMgr != nil {
		c.connMgr.Set(clusterID, restCfg, fingerprint)
	}
	// Watch close → re-probe, whether or not this cluster syncs.
	c.ensureSentinel(clusterID, restCfg, fingerprint)

	working.Server = server
	working.Principal = principal
	working.LastConnectedAt = &now
	c.recordAttempt(ctx, client, obj.ID, true, ReasonConnected, "")
	conds.set(liveCondition(ConditionConnected, ConditionTrue, ReasonConnected, ""))

	// Both steps are gated on a CONFIRMED UID — a transient disconnect (UID == nil) must
	// never prune the existing cache. Store errors are non-fatal so they can't lose the
	// just-observed connection status; the next reconcile retries.
	if server.UID != nil {
		if err := c.ensureClusterCache(ctx, clusterID, obj.ID, *server.UID); err != nil {
			slog.Warn("clustercontroller: ensure cache child", "cluster", clusterID, "err", err)
		}
		c.pruneSupersededCaches(ctx, clusterID, obj.ID, *server.UID)
	}

	phase, msg := c.check(ctx, restCfg)
	conds.set(healthCondition(phase, msg))
	// A nil error resets beehive's backoff; the steady cadence is the health poll.
	return healthProbeInterval, nil
}

// observeKubeconfig records the live observation and reports whether the context is present.
// A departed context keeps its last-known names with IsPresent=false, so an orphaned record
// stays identifiable. A non-kubeconfig-sourced record is left untouched (returns false).
func (c *ClusterCoreController) observeKubeconfig(obj *beehive.Object[ClusterSpec, ClusterStatus], working *ClusterStatus) bool {
	kc := obj.Spec.Source.Kubeconfig
	if kc == nil {
		return false
	}
	if cfg := c.cfgSource.Get(); cfg != nil {
		if kctx, ok := cfg.Contexts[kc.Context]; ok && kctx != nil {
			working.Source.Kubeconfig = &ClusterStatusSourceKubeconfig{
				Cluster:   kctx.Cluster,
				User:      kctx.AuthInfo,
				IsPresent: true,
				IsDefault: kc.Context == cfg.CurrentContext,
			}
			return true
		}
	}
	// Departed (or no kubeconfig): keep last-known names, mark absent.
	prev := ClusterStatusSourceKubeconfig{}
	if working.Source.Kubeconfig != nil {
		prev = *working.Source.Kubeconfig
	}
	prev.IsPresent = false
	prev.IsDefault = false
	working.Source.Kubeconfig = &prev
	return false
}

// observeConnectFailure records the failed-connect conditions and returns err, which is how
// beehive applies its backoff (1s ×2, capped by WithMaxRetryInterval) and clears it on the
// next success. Out-of-band reprobes bypass beehive's worker, so they can't disturb it.
func (c *ClusterCoreController) observeConnectFailure(conds *conditionSet, reason string, err error) error {
	conds.set(liveCondition(ConditionConnected, ConditionFalse, reason, err.Error()))
	conds.set(liveCondition(ConditionHealthy, ConditionUnknown, ReasonNoConnection, ""))
	return err
}

// ensureClusterCache creates the ClusterCache child for one kube-system UID if absent. The
// name keys beehive's per-kind uniqueness, so concurrent reconciles converge on one cache
// per (cluster, UID). No wait-for-GC branch is needed: this name is never deletion-pending
// during a successful probe (a pause keeps the row, a prune targets another UID, and a
// cluster-delete cascade makes the cluster ineligible before converge gets here).
func (c *ClusterCoreController) ensureClusterCache(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID, uid string) error {
	name := ClusterCacheName(clusterID, uid)
	_, _, err := c.cacheClient.GetOrCreate(ctx, name, ClusterCacheSpec{ServerUID: uid},
		beehive.WithOwner(ownerID),
		// GC can't collect the row until the cache controller has deleted the .db.
		beehive.WithFinalizers(cacheFilesFinalizer),
	)
	return err
}

// pruneSupersededCaches requests deletion of every cache whose ServerUID differs from
// activeUID — those left behind by a physical-identity change. A soft request: the
// finalizer holds the row until the subtree drains and the file is gone, so this never
// races the cleanup. Only ever called with a confirmed post-probe UID; errors are logged.
func (c *ClusterCoreController) pruneSupersededCaches(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID, activeUID string) {
	// Kind-scoped and decoded, so no untyped-ref filter and no per-child Get.
	caches, err := c.cacheClient.ListOwnedObjects(ctx, ownerID)
	if err != nil {
		slog.Warn("clustercontroller: list caches for prune", "cluster", clusterID, "err", err)
		return
	}
	for _, cacheObj := range caches {
		if cacheObj.Spec.ServerUID == activeUID || cacheObj.DeletionRequestedAt != nil {
			continue // the active cache, or one already being deleted
		}
		if err := c.cacheClient.Delete(ctx, cacheObj.ID); err != nil {
			slog.Warn("clustercontroller: delete superseded cache", "cluster", clusterID, "cache", cacheObj.ID, "err", err)
		}
	}
}

// clusterIDFromObj returns a Cluster object's ClusterID: its beehive ObjectID.
func clusterIDFromObj(obj *beehive.Object[ClusterSpec, ClusterStatus]) ClusterID {
	return ClusterID(obj.ID)
}

// healthCondition maps one health-probe outcome onto the Healthy condition.
func healthCondition(phase HealthPhase, msg *string) Condition {
	var message string
	if msg != nil {
		message = *msg
	}
	switch phase {
	case HealthPhaseHealthy:
		return liveCondition(ConditionHealthy, ConditionTrue, ReasonReady, message)
	case HealthPhaseDegraded:
		return liveCondition(ConditionHealthy, ConditionFalse, ReasonReadyzFailed, message)
	case HealthPhaseUnreachable:
		return liveCondition(ConditionHealthy, ConditionFalse, ReasonUnreachable, message)
	default:
		return liveCondition(ConditionHealthy, ConditionUnknown, ReasonNoConnection, message)
	}
}

// probeCluster dials the cluster for server version, kube-system UID, and the authenticated
// username.
func probeCluster(ctx context.Context, cfg *rest.Config) (ClusterServer, ClusterPrincipal, error) {
	probeCfg := rest.CopyConfig(cfg)
	probeCfg.Timeout = connectionProbeTimeout
	clientset, err := kubernetes.NewForConfig(probeCfg)
	if err != nil {
		return ClusterServer{}, ClusterPrincipal{}, err
	}
	ver, err := clientset.Discovery().ServerVersion()
	if err != nil {
		return ClusterServer{}, ClusterPrincipal{}, err
	}
	ns, err := clientset.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return ClusterServer{}, ClusterPrincipal{}, err
	}
	ssr, err := clientset.AuthenticationV1().SelfSubjectReviews().Create(
		ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return ClusterServer{}, ClusterPrincipal{}, err
	}
	uid := string(ns.UID)
	return ClusterServer{UID: &uid, Version: &ver.GitVersion},
		ClusterPrincipal{Username: &ssr.Status.UserInfo.Username}, nil
}

// checkServerHealth probes the API server's own readiness endpoint.
func checkServerHealth(ctx context.Context, restCfg *rest.Config) (HealthPhase, *string) {
	cfg := rest.CopyConfig(restCfg)
	cfg.Timeout = healthProbeTimeout
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		msg := err.Error()
		return HealthPhaseUnknown, &msg
	}
	body, err := clientset.Discovery().RESTClient().
		Get().AbsPath("/readyz").Param("verbose", "true").DoRaw(ctx)
	if err == nil {
		return HealthPhaseHealthy, nil
	}
	if len(body) > 0 {
		msg := strings.TrimSpace(string(body))
		return HealthPhaseDegraded, &msg
	}
	msg := err.Error()
	return HealthPhaseUnreachable, &msg
}
