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
// It subscribes to the kubeconfig watcher (in StartBackground) and re-reconciles
// all clusters on a change, so edits propagate promptly; the status write then
// wakes each ClusterCache dependent. Reconciles of one cluster serialize on that
// cluster's own lock and re-read fresh under it (see Reconcile) so the status-owned
// observation can't be clobbered by a stale snapshot; reconciles of different
// clusters run in parallel (see reconcileMu).
type ClusterCoreController struct {
	cfgSource   KubeConfigSource
	coreClient  beehive.Client[ClusterSpec, ClusterStatus]
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	connMgr     *ConnectionManager
	ctrlClient  beehive.ControllerClient[ClusterStatus]

	// pokeSvc is the resync bus; nil disables poke-driven re-probes (tests).
	pokeSvc *poke.Service

	// retryCh is the in-process targeted-retry bus: Service.RetryConnection sends
	// a ClusterID and the background worker re-probes that one cluster out-of-band.
	// Carries a payload (the target id), so it can't ride the payload-less poke bus.
	retryCh chan ClusterID

	// bgWG/bgCancel own the single background worker (StartBackground): it drains
	// the targeted-retry bus (one cluster), the resync poke bus (all clusters), and
	// the kubeconfig watcher (re-reconcile all on a change) in one select.
	bgWG     sync.WaitGroup
	bgCancel context.CancelFunc

	// Connection sentinels: one long-lived liveness watch per connected cluster,
	// owned by converge (started on a successful probe, stopped on ineligibility/
	// failure). A watch closing is the earliest connection-loss signal, so the
	// sentinel re-probes out-of-band — fast detection owned by the connection
	// controller itself, independent of whether sync is running. See
	// connection_sentinel.go.
	sentinelWatch SentinelWatchFunc
	// sentinelMu guards both the sentinels map and bgCtx (the base context the
	// sentinel goroutines derive from, published by StartBackground).
	sentinelMu sync.Mutex
	sentinels  map[ClusterID]*connSentinel
	sentinelWG sync.WaitGroup
	bgCtx      context.Context

	// reconcileMu guards the per-cluster reconcile locks in reconcileLocks. The
	// invariant those locks enforce is per object: beehive status writes carry no
	// resourceVersion guard, so two reconciles of the SAME cluster must not interleave
	// their read-modify-write, or a stale snapshot clobbers a newer observation. Two
	// reconciles of DIFFERENT clusters share nothing — each writes its own object, and
	// every other piece of state converge touches is separately guarded (connMgr's own
	// RWMutex, sentinels under sentinelMu, probing under probeMu) — so they run in
	// parallel. That parallelism is what keeps one unreachable cluster's dial timeout
	// from delaying every cluster behind it at startup, when the beehive startup pass
	// and the kubeconfig watcher's first snapshot both enqueue every cluster at once.
	//
	// A cluster's lock is held across its own probe, which is intended: it is pointless
	// to probe one cluster twice concurrently, and it keeps setProbing's true/false
	// pairing (and so the webview's "checking now") correct. Entries are never removed —
	// a deleted cluster leaks one mutex, and the map is bounded by the number of
	// clusters this process has ever seen (a desktop app's kube-contexts, so tens) —
	// which avoids the race of freeing a lock another goroutine is about to take.
	reconcileMu    sync.Mutex
	reconcileLocks map[ClusterID]*sync.Mutex

	// probeMu guards the in-flight probe set and serializes hub publishes with a new
	// subscriber's current-value read (WatchProbe), so a subscriber never misses a
	// transition nor double-counts one straddling its subscribe. probing holds the ids
	// whose network probe is currently running (converge, between the eligibility gate
	// and the probe/health round-trips returning); probeHub fans each transition out,
	// so the webview can show a definite "checking now" — see WatchProbe /
	// Schedule.Probing.
	probeMu  sync.Mutex
	probing  map[ClusterID]bool
	probeHub *broadcast.Hub[probeUpdate]
	probeTx  *broadcast.Sender[probeUpdate]

	probe ProbeFunc
	check CheckFunc
}

// probeUpdate is one in-flight-probe transition fanned out over probeHub: Active
// true when a cluster's network probe starts, false when it returns.
type probeUpdate struct {
	ID     ClusterID
	Active bool
}

// probeHubCapacity bounds the probe-transition fan-out buffer. At most
// clusterProbeConcurrency probes run at once and each publishes two transitions, so
// a small buffer is ample.
const probeHubCapacity = 16

// clusterProbeConcurrency caps how many clusters are probed at once — the beehive
// worker count for the Cluster kind (see Service.New) and the size of reconcileAll's
// pool, since both spend their time in the same place: one cluster's network probe +
// health check. Bounded rather than unlimited so a laptop with many kube-contexts
// doesn't open a TLS handshake per context at once; wide enough that a handful of
// unreachable clusters can't stall the reachable ones behind them.
const clusterProbeConcurrency = 8

// retryBufferSize bounds the targeted-retry bus. A full buffer means a retry is
// already queued, so further Reprobe calls are dropped (non-blocking).
const retryBufferSize = 64

// pokeReprobeTimeout bounds one cluster's poke-driven re-probe (its own probe +
// health round-trips are separately capped inside probe/check).
const pokeReprobeTimeout = connectionProbeTimeout + healthProbeTimeout

// NewClusterCoreController builds the controller from the shared runtime plus its own
// specifics. It mints the Cluster + ClusterCache clients from rt.bh (the former lets it
// enumerate clusters for a poke-driven re-probe, the latter creates ClusterCache
// children); rt.connMgr may be nil (no connection tracking) and rt.pokeSvc may be nil
// (no poke-driven re-probe). cfgSource is the kubeconfig source; probe and check default
// to the real network implementations, tests inject fakes.
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

// WatchProbe streams whether id's connection probe is in flight: current-on-subscribe
// (a subscriber opening mid-probe sees true), then one value per transition. The
// channel closes when ctx ends. The current read and hub subscribe happen under
// probeMu (setProbing's lock), so no transition is lost in the gap. Backs the Probing
// field merged into ClusterScheduleWatch.
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
			// A slow consumer missed transitions the hub overwrote. Re-read the
			// authoritative flag instead of carrying on: the values we lost may have
			// included this cluster's probe FINISHING, and inferring "still probing" from
			// a gap would leave the row stuck on "checking now" until the next probe.
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

// recordAttempt appends one probe outcome to the cluster's beehive event log
// (category ConnectionEventCategory). beehive coalesces consecutive outcomes sharing
// (category, type, reason) into one run, so a repeated failure bumps the run's
// count/window instead of writing status — avoiding the per-probe chatter a
// status-stored history would cause. Best-effort: a write failure is logged, so it
// neither fails the reconcile nor disturbs backoff (the outcome is carried by the
// conditions the caller sets).
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

// SetControllerClient injects the status-write client from beehive.Register. It
// backs the out-of-band reconciles (reconcileAll/reprobeOne); the reconcile path
// uses the client beehive passes into Reconcile. Call once, before the control
// plane starts.
func (c *ClusterCoreController) SetControllerClient(cl beehive.ControllerClient[ClusterStatus]) {
	c.ctrlClient = cl
}

// StartBackground launches the controller's single out-of-band worker. It drains
// three sources in one select: the in-process targeted-retry bus (Reprobe →
// reprobeOne, one cluster), the resync poke bus (reconcileAll on OS resume /
// network-on), and the kubeconfig watcher (reconcileAll on a change, so
// presence/isDefault updates propagate promptly). A nil pokeSvc leaves the poke
// arm dormant (the channel stays nil and never fires), so the retry + watcher arms
// still work. Call after the control plane has started; pair with StopBackground.
func (c *ClusterCoreController) StartBackground() {
	ctx, cancelCtx := context.WithCancel(context.Background())

	// Publish the base context the connection sentinels anchor to. Until this is
	// set, ensureSentinel skips (a reconcile may run between bh.Start and here); the
	// next reconcile starts the sentinel once the worker is up.
	c.sentinelMu.Lock()
	c.bgCtx = ctx
	c.sentinelMu.Unlock()

	var pokeCh <-chan poke.Signal
	cancelSub := func() {}
	if c.pokeSvc != nil {
		pokeCh, cancelSub = c.pokeSvc.Subscribe()
	}

	// Subscribe to the kubeconfig watcher: on every change re-reconcile all clusters
	// so presence/isDefault observations (and the sync start/stop the cache
	// controller derives from them) update promptly. The stream is current-on-subscribe,
	// so the first value reconciles everything at startup.
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

// Reprobe requests an immediate out-of-band re-probe of one cluster. Non-blocking:
// dropped if the bus buffer is full (a retry is already queued). Two producers: the
// connection sentinel on a liveness-watch close, and the user-initiated retry
// (Service.RetryConnection). Both want an immediate, backoff-neutral probe, so
// neither perturbs beehive's scheduled backoff ladder.
func (c *ClusterCoreController) Reprobe(id ClusterID) {
	select {
	case c.retryCh <- id:
	default:
	}
}

// reconcileAll re-runs the reconcile for every (non-deleting) cluster. Driven by the
// resync poke bus (OS resume / network-on) and the kubeconfig watcher (presence /
// isDefault / name changes) — and, because that subscription is current-on-subscribe,
// once at startup. Reconcile re-reads each object fresh, re-observes the kubeconfig,
// and gates eligibility — so a now-ineligible (e.g. just-departed) cluster still
// updates its observation and conditions, and the status write wakes its ClusterCache
// dependent. The List snapshot only enumerates ids.
//
// Clusters are reconciled concurrently, clusterProbeConcurrency at a time: each one
// spends its time in a network probe, and reconciling them in sequence would make a
// single unreachable cluster's dial timeout delay every cluster behind it. Reconcile
// locks per cluster, so the fan-out is safe.
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

// reprobeOne re-runs the connection reconcile for one cluster by id, forcing an
// immediate probe out-of-band. Driven by the targeted-retry bus
// (Service.RetryConnection). An unknown id is a no-op (it may have been deleted
// between the mutation and the worker draining the bus).
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

// reprobeObj forces an immediate out-of-band reconcile of one cluster object
// (shared by reconcileAll and reprobeOne). It does not pre-gate on eligibility:
// Reconcile re-reads the object fresh and re-observes the kubeconfig, deciding
// eligibility there from current state (a stale pre-gate could wrongly skip a
// cluster whose observation was written concurrently). An ineligible cluster just
// gets a cheap status-only reconcile.
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
		// Any other read failure ends the pass. Falling through would converge on the
		// snapshot this reconcile was handed and then UpdateStatus it — with no
		// resourceVersion guard, that is precisely the clobber of a newer observation the
		// lock and this re-read exist to prevent.
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
	// One transaction for the whole pass: the conditions land together with the status
	// they describe, so a watcher never sees Connected=True beside a stale Server, nor
	// half of a Connected/Healthy pair. When the status is unchanged we still settle the
	// generation — conditions are their own rows and don't advance the handshake, so a
	// pass whose only output is a condition would otherwise leave the object unsettled
	// and re-enqueued by the owed pass forever.
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
	// A probe/resolve failure is returned as an error so beehive applies its
	// exponential backoff; the status above is already recorded for the UI.
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

	// Observe the kubeconfig live and record it on status (a departed context keeps
	// its last-known names, IsPresent=false). The freshly-observed presence drives
	// this reconcile's gate, so it never lags the status we're about to write.
	present := c.observeKubeconfig(obj, working)

	if !connectionEligible(obj, present) {
		c.teardownConnection(clusterID)
		conds.set(liveCondition(ConditionConnected, ConditionFalse, ReasonInactive, ""))
		conds.set(liveCondition(ConditionHealthy, ConditionUnknown, ReasonInactive, ""))
		return 0, nil
	}

	// Eligible: mark the probe in flight so the webview shows a definite "checking now"
	// for the round-trip's duration (resolve/probe/health can take seconds). The defer
	// clears it the moment converge returns — success, failure, or resolve error alike.
	c.setProbing(clusterID, true)
	defer c.setProbing(clusterID, false)

	// now stamps LastConnectedAt on success; each probe outcome is recorded into
	// the event log separately (beehive stamps the event's own window).
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

	// The fingerprint is computed once here and shared: this is the only place that can
	// see the kubeconfig's raw proxy-url, which ConfigFingerprint needs and no downstream
	// reader has. The sentinel keys its liveness watch on it, and the sync controllers
	// read it back off the ConnectionManager to detect a credential rotation.
	fingerprint := ConfigFingerprint(restCfg, ContextProxyURL(c.cfgSource.Get(), contextName))
	if c.connMgr != nil {
		c.connMgr.Set(clusterID, restCfg, fingerprint)
	}
	// Connected: hold a liveness watch open so a dropped connection is detected
	// fast (watch close → re-probe), regardless of whether this cluster syncs. Keyed
	// by the connection-config fingerprint so a credential rotation restarts it.
	c.ensureSentinel(clusterID, restCfg, fingerprint)

	working.Server = server
	working.Principal = principal
	working.LastConnectedAt = &now
	c.recordAttempt(ctx, client, obj.ID, true, ReasonConnected, "")
	conds.set(liveCondition(ConditionConnected, ConditionTrue, ReasonConnected, ""))

	// The probe confirmed the kube-system UID: ensure a ClusterCache exists for that
	// identity, then delete any cache left behind by a migration (a superseded UID).
	// Both are gated on a confirmed UID — never on a transient disconnect (server.UID
	// == nil), which must not prune the existing cache. A store error is non-fatal: the
	// status write below still records the connection, and the next reconcile retries.
	if server.UID != nil {
		if err := c.ensureClusterCache(ctx, clusterID, obj.ID, *server.UID); err != nil {
			slog.Warn("clustercontroller: ensure cache child", "cluster", clusterID, "err", err)
		}
		c.pruneSupersededCaches(ctx, clusterID, obj.ID, *server.UID)
	}

	phase, msg := c.check(ctx, restCfg)
	conds.set(healthCondition(phase, msg))
	// Connected — a nil error makes beehive reset any accumulated backoff; the
	// steady cadence is the periodic health poll.
	return healthProbeInterval, nil
}

// observeKubeconfig writes the live kubeconfig observation for obj's context into
// working.Source.Kubeconfig and reports whether the context is currently present.
// A departed context keeps its last-known cluster/user names (already on working,
// cloned from the prior status) with IsPresent=false, so an orphaned record stays
// identifiable. A non-kubeconfig-sourced record is left untouched (returns false).
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

// observeConnectFailure records the failed-connect conditions and returns err so the
// caller surfaces it from Reconcile — how beehive applies its exponential backoff
// (base 1s, ×2, capped by WithMaxRetryInterval) and resets it on the next success.
// Out-of-band reprobes bypass beehive's worker, so their errors don't disturb the
// scheduled cadence.
func (c *ClusterCoreController) observeConnectFailure(conds *conditionSet, reason string, err error) error {
	conds.set(liveCondition(ConditionConnected, ConditionFalse, reason, err.Error()))
	conds.set(liveCondition(ConditionHealthy, ConditionUnknown, ReasonNoConnection, ""))
	return err
}

// ensureClusterCache creates the ClusterCache child for one identity (kube-system
// UID) if it doesn't already exist. The name ("{clusterID}/{uid}") keys beehive's
// per-kind name uniqueness, so concurrent reconciles racing the create converge on one
// cache per (cluster, UID). On a migration the new UID's cache is created here and
// pruneSupersededCaches removes the old one. GetOrCreate does the read-or-create
// atomically and returns an existing row untouched, so a concurrent reconcile can't
// duplicate the child. Unlike the cache's own deletion this has no wait-for-GC branch
// on a deletion-pending row: this name is never deletion-pending during a successful
// probe (pause keeps the cache row; a prune targets a different UID; a cluster-delete
// cascade makes the cluster ineligible so converge never reaches here), so an existing
// row — always finalizer-bearing since we created it that way — is simply success.
func (c *ClusterCoreController) ensureClusterCache(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID, uid string) error {
	name := ClusterCacheName(clusterID, uid)
	_, _, err := c.cacheClient.GetOrCreate(ctx, name, ClusterCacheSpec{ServerUID: uid},
		beehive.WithOwner(ownerID),
		// Gate deletion on the cache controller flushing this cache's on-disk
		// files (UID-switch prune or cluster-delete cascade): GC won't collect the
		// row until the finalizer is cleared, so the .db file can't leak.
		beehive.WithFinalizers(cacheFilesFinalizer),
	)
	return err
}

// pruneSupersededCaches requests deletion of every ClusterCache owned by ownerID
// whose ServerUID differs from activeUID — the caches left behind when the cluster's
// physical identity changed. Deletion is a soft request; the ClusterCache's finalizer
// holds the row until the cache controller's subtree has drained and it has deleted the
// on-disk file, so this never races the file cleanup. Only ever called with a
// confirmed activeUID (post-probe). Errors are logged, not fatal: the next reconcile
// retries.
func (c *ClusterCoreController) pruneSupersededCaches(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID, activeUID string) {
	// ListOwnedObjects is kind-scoped to the cache client and returns the children
	// decoded, so it enumerates ownerID's ClusterCaches directly — no untyped-ref
	// kind filter and no per-child Get.
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

// clusterIDFromObj returns the ClusterID of a Cluster object: its beehive
// ObjectID.
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

// probeCluster dials the cluster and discovers server version, kube-system UID,
// and the authenticated username via SelfSubjectReview.
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
