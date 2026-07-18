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
	"slices"
	"strings"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"
	"github.com/amorey/gochan/broadcast"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/engine"
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
// wakes each ClusterCache dependent. Reconciles serialize on writeMu and re-read
// fresh (see Reconcile) so the status-owned observation can't be clobbered by a
// stale snapshot.
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

	// writeMu serializes reconciles. Both beehive-scheduled and out-of-band reconciles
	// take it and re-read the object's status fresh under it, so a stale snapshot can't
	// clobber a newer observation — beehive status writes carry no resourceVersion
	// guard. Held across the probe; for a desktop app's handful of clusters the
	// serialized probing is acceptable.
	writeMu sync.Mutex

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

// probeHubCapacity bounds the probe-transition fan-out buffer. Probes serialize
// on writeMu (one in flight at a time) and are seconds apart, so a small buffer
// is ample.
const probeHubCapacity = 16

// retryBufferSize bounds the targeted-retry bus. A full buffer means a retry is
// already queued, so further Reprobe calls are dropped (non-blocking).
const retryBufferSize = 64

// pokeReprobeTimeout bounds one cluster's poke-driven re-probe (its own probe +
// health round-trips are separately capped inside probe/check).
const pokeReprobeTimeout = connectionProbeTimeout + healthProbeTimeout

// NewClusterCoreController builds the controller. coreClient lets it enumerate
// clusters for a poke-driven re-probe; connMgr may be nil (no connection
// tracking); pokeSvc may be nil (no poke-driven re-probe). probe and check
// default to the real network implementations; tests inject fakes.
func NewClusterCoreController(
	cfgSource KubeConfigSource,
	coreClient beehive.Client[ClusterSpec, ClusterStatus],
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus],
	connMgr *ConnectionManager,
	pokeSvc *poke.Service,
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
		cfgSource:     cfgSource,
		coreClient:    coreClient,
		cacheClient:   cacheClient,
		connMgr:       connMgr,
		pokeSvc:       pokeSvc,
		retryCh:       make(chan ClusterID, retryBufferSize),
		probe:         probe,
		check:         check,
		sentinelWatch: watchKubeSystem,
		sentinels:     make(map[ClusterID]*connSentinel),
		probing:       make(map[ClusterID]bool),
		probeHub:      probeHub,
		probeTx:       probeHub.Sender(),
	}
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
			select {
			case <-ctx.Done():
				return
			case u, ok := <-rx.Chan():
				if !ok {
					return
				}
				if u.ID != id {
					continue
				}
				select {
				case out <- u.Active:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// maxAttemptMessageLen caps a recorded event message's length.
const maxAttemptMessageLen = 200

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
	err := client.RecordEvent(ctx, id, beehive.EventSpec{
		Category: ConnectionEventCategory,
		Type:     typ,
		Reason:   reason,
		Message:  truncateMessage(message),
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("clustercontroller: record connection event", "cluster", id, "reason", reason, "err", err)
	}
}

// truncateMessage caps s at maxAttemptMessageLen bytes, appending an ellipsis
// when it overflows. (Byte-bounded; error strings are effectively ASCII.)
func truncateMessage(s string) string {
	if len(s) <= maxAttemptMessageLen {
		return s
	}
	return s[:maxAttemptMessageLen] + "…"
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
	// so presence/isDefault observations (and the engine start/stop the cache
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
// isDefault / name changes). Reconcile re-reads each object fresh, re-observes the
// kubeconfig, and gates eligibility — so a now-ineligible (e.g. just-departed)
// cluster still updates its observation and conditions, and the status write wakes
// its ClusterCache dependent. The List snapshot only enumerates ids.
func (c *ClusterCoreController) reconcileAll(baseCtx context.Context) {
	objs, err := c.coreClient.List(baseCtx)
	if err != nil {
		if baseCtx.Err() == nil {
			slog.Warn("clustercontroller: list clusters for re-reconcile", "err", err)
		}
		return
	}
	for _, obj := range objs {
		if baseCtx.Err() != nil {
			return // shutting down
		}
		if obj.DeletionRequestedAt != nil {
			continue
		}
		c.reprobeObj(baseCtx, obj)
	}
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
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Re-read fresh under the lock so this reconcile acts on the latest status, not the
	// snapshot it was handed. Status writes have no resourceVersion guard, so a stale
	// snapshot would otherwise clobber a newer source observation.
	if fresh, err := c.coreClient.Get(ctx, obj.ID); err == nil {
		obj = fresh
	} else if errors.Is(err, beehive.ErrNotFound) {
		return beehive.Result{}, nil
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
		Conditions:      slices.Clone(loaded.Conditions),
	}

	requeueAfter, convergeErr := c.converge(ctx, client, obj, &working)

	// Persist observations before surfacing any failure — the status write must land
	// even when the probe failed. The probe-outcome event was already recorded by
	// converge.
	if !ClusterStatusEqual(loaded, working) {
		if err := client.UpdateStatus(ctx, obj.ID, obj.Generation, working); err != nil {
			return beehive.Result{}, err
		}
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
func (c *ClusterCoreController) converge(ctx context.Context, client beehive.ControllerClient[ClusterStatus], obj *beehive.Object[ClusterSpec, ClusterStatus], working *ClusterStatus) (time.Duration, error) {
	gen := obj.Generation
	conds := &working.Conditions

	clusterID := clusterIDFromObj(obj)

	// Observe the kubeconfig live and record it on status (a departed context keeps
	// its last-known names, IsPresent=false). The freshly-observed presence drives
	// this reconcile's gate, so it never lags the status we're about to write.
	present := c.observeKubeconfig(obj, working)

	if !connectionEligible(obj, present) {
		c.teardownConnection(clusterID)
		SetCondition(conds, Condition{
			Type:               ConditionConnected,
			Status:             ConditionFalse,
			Reason:             ReasonInactive,
			ObservedGeneration: gen,
		})
		SetCondition(conds, Condition{
			Type:               ConditionHealthy,
			Status:             ConditionUnknown,
			Reason:             ReasonInactive,
			ObservedGeneration: gen,
		})
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
		return 0, c.observeConnectFailure(conds, gen, ReasonResolveFailed, err)
	}

	server, principal, err := c.probe(ctx, restCfg)
	if err != nil {
		c.teardownConnection(clusterID)
		c.recordAttempt(ctx, client, obj.ID, false, ReasonProbeFailed, err.Error())
		return 0, c.observeConnectFailure(conds, gen, ReasonProbeFailed, err)
	}

	if c.connMgr != nil {
		c.connMgr.Set(clusterID, restCfg)
	}
	// Connected: hold a liveness watch open so a dropped connection is detected
	// fast (watch close → re-probe), regardless of whether this cluster syncs. Keyed
	// by the connection-config fingerprint so a credential rotation restarts it.
	c.ensureSentinel(clusterID, restCfg,
		engine.ConfigFingerprint(restCfg, engine.ContextProxyURL(c.cfgSource.Get(), contextName)))

	working.Server = server
	working.Principal = principal
	working.LastConnectedAt = &now
	c.recordAttempt(ctx, client, obj.ID, true, ReasonConnected, "")
	SetCondition(conds, Condition{
		Type:               ConditionConnected,
		Status:             ConditionTrue,
		Reason:             ReasonConnected,
		ObservedGeneration: gen,
	})

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
	SetCondition(conds, healthCondition(phase, msg, gen))
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
func (c *ClusterCoreController) observeConnectFailure(conds *[]Condition, gen int64, reason string, err error) error {
	SetCondition(conds, Condition{
		Type: ConditionConnected, Status: ConditionFalse,
		Reason: reason, Message: err.Error(), ObservedGeneration: gen,
	})
	SetCondition(conds, Condition{
		Type: ConditionHealthy, Status: ConditionUnknown,
		Reason: ReasonNoConnection, ObservedGeneration: gen,
	})
	return err
}

// ensureClusterCache creates the ClusterCache child for one identity (kube-system
// UID) if it doesn't already exist. The slug ("{clusterID}/{uid}") keys beehive's
// UNIQUE(slug) dedup, so concurrent reconciles racing the create converge on one
// cache per (cluster, UID). On a migration the new UID's cache is created here and
// pruneSupersededCaches removes the old one.
func (c *ClusterCoreController) ensureClusterCache(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID, uid string) error {
	slug := ClusterCacheSlug(clusterID, uid)
	_, err := c.cacheClient.GetBySlug(ctx, slug)
	if err == nil {
		return nil // already exists
	}
	if !errors.Is(err, beehive.ErrNotFound) {
		return err
	}
	_, err = c.cacheClient.Create(ctx, ClusterCacheSpec{ServerUID: uid},
		beehive.WithSlug(slug),
		beehive.WithOwner(ownerID),
		// Gate deletion on the cache controller flushing this cache's on-disk
		// files (UID-switch prune or cluster-delete cascade): GC won't collect the
		// row until the finalizer is cleared, so the .db file can't leak.
		beehive.WithFinalizers(cacheFilesFinalizer),
	)
	if err != nil {
		// A concurrent reconcile may have created the child between our Get and Create
		// — treat an existing child as success.
		if _, gerr := c.cacheClient.GetBySlug(ctx, slug); gerr == nil {
			return nil
		}
		return err
	}
	return nil
}

// pruneSupersededCaches requests deletion of every ClusterCache owned by ownerID
// whose ServerUID differs from activeUID — the caches left behind when the cluster's
// physical identity changed. Deletion is a soft request; the ClusterCache's finalizer
// holds the row until the cache controller has stopped its engine and deleted the
// on-disk file, so this never races the file cleanup. Only ever called with a
// confirmed activeUID (post-probe). Errors are logged, not fatal: the next reconcile
// retries.
func (c *ClusterCoreController) pruneSupersededCaches(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID, activeUID string) {
	// ListOwned is scoped to the client's own kind, so run it on the owner's (Cluster)
	// client; the children are then read/deleted through the cache client.
	refs, err := c.coreClient.ListOwned(ctx, ownerID)
	if err != nil {
		slog.Warn("clustercontroller: list caches for prune", "cluster", clusterID, "err", err)
		return
	}
	for _, ref := range refs {
		if ref.Kind != ClusterCacheGroupKind.Kind {
			continue
		}
		cacheObj, err := c.cacheClient.Get(ctx, ref.ID)
		if err != nil {
			continue
		}
		if cacheObj.Spec.ServerUID == activeUID || cacheObj.DeletionRequestedAt != nil {
			continue // the active cache, or one already being deleted
		}
		if err := c.cacheClient.Delete(ctx, ref.ID); err != nil {
			slog.Warn("clustercontroller: delete superseded cache", "cluster", clusterID, "cache", ref.ID, "err", err)
		}
	}
}

// clusterIDFromObj returns the ClusterID of a Cluster object: its beehive
// ObjectID.
func clusterIDFromObj(obj *beehive.Object[ClusterSpec, ClusterStatus]) ClusterID {
	return ClusterID(obj.ID)
}

// healthCondition maps one health-probe outcome onto the Healthy condition.
func healthCondition(phase HealthPhase, msg *string, gen int64) Condition {
	cond := Condition{Type: ConditionHealthy, ObservedGeneration: gen}
	if msg != nil {
		cond.Message = *msg
	}
	switch phase {
	case HealthPhaseHealthy:
		cond.Status, cond.Reason = ConditionTrue, ReasonReady
	case HealthPhaseDegraded:
		cond.Status, cond.Reason = ConditionFalse, ReasonReadyzFailed
	case HealthPhaseUnreachable:
		cond.Status, cond.Reason = ConditionFalse, ReasonUnreachable
	default:
		cond.Status, cond.Reason = ConditionUnknown, ReasonNoConnection
	}
	return cond
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
