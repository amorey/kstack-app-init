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

	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

const (
	// connectionInitialBackoff and connectionMaxBackoff pace reconnect
	// attempts after probe failures: doubling waits, capped.
	connectionInitialBackoff = time.Second
	connectionMaxBackoff     = 2 * time.Minute

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
//  2. Mirrors ClusterCoreSpec.SourceObs into ClusterCoreStatus.Source.Kubeconfig
//     (the kubeconfig observation written by the kubeconfig importer).
//  3. Gates on eligibility (active + kubeconfig present + not being deleted).
//  4. Resolves REST credentials from the current kubeconfig.
//  5. Probes the connection (server version + UID + principal).
//  6. Probes server health (readyz endpoint).
//  7. Writes all observations to ClusterCoreStatus via UpdateStatus.
//  8. Updates connMgr: Set on success, Delete on failure or ineligibility.
type ClusterCoreController struct {
	cfgSource   KubeConfigSource
	coreClient  beehive.Client[ClusterCoreSpec, ClusterCoreStatus]
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	connMgr     *ConnectionManager
	ctrlClient  beehive.ControllerClient[ClusterCoreStatus]

	// pokeSvc is the resync bus; nil disables poke-driven re-probes (tests).
	pokeSvc *poke.Service

	// retryCh is the in-process targeted-retry bus: Service.RetryConnection sends
	// a ClusterID and the background worker re-probes that one cluster out-of-band.
	// Carries a payload (the target id), so it can't ride the payload-less poke bus.
	retryCh chan ClusterID

	// bgWG/bgCancel own the single background worker (StartBackground): it drains
	// both the targeted-retry bus (one cluster) and the resync poke bus (all
	// clusters) in one select, since both just trigger out-of-band re-probes.
	bgWG     sync.WaitGroup
	bgCancel context.CancelFunc

	probe ProbeFunc
	check CheckFunc
}

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
	coreClient beehive.Client[ClusterCoreSpec, ClusterCoreStatus],
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
	return &ClusterCoreController{
		cfgSource:   cfgSource,
		coreClient:  coreClient,
		cacheClient: cacheClient,
		connMgr:     connMgr,
		pokeSvc:     pokeSvc,
		retryCh:     make(chan ClusterID, retryBufferSize),
		probe:       probe,
		check:       check,
	}
}

// SetControllerClient injects the status-write client obtained from
// beehive.Register. It backs the out-of-band re-probes (reprobeAll/reprobeOne);
// the reconcile path uses the client beehive passes into Reconcile instead. Call
// once, before the control plane starts.
func (c *ClusterCoreController) SetControllerClient(cl beehive.ControllerClient[ClusterCoreStatus]) {
	c.ctrlClient = cl
}

// StartBackground launches the controller's single out-of-band worker. It drains
// two re-probe sources in one select: the in-process targeted-retry bus (Reprobe →
// reprobeOne, one cluster) and, when a poke bus is configured, the resync poke bus
// (reprobeAll, every eligible cluster on OS resume / network-on). A nil pokeSvc
// leaves the poke arm dormant (the channel stays nil and never fires), so the
// retry bus still works. Call after the control plane has started; pair with
// StopBackground.
func (c *ClusterCoreController) StartBackground() {
	ctx, cancelCtx := context.WithCancel(context.Background())

	var pokeCh <-chan poke.Signal
	cancelSub := func() {}
	if c.pokeSvc != nil {
		pokeCh, cancelSub = c.pokeSvc.Subscribe()
	}
	c.bgCancel = func() { cancelSub(); cancelCtx() }

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
				c.reprobeAll(ctx)
			}
		}
	})
}

// StopBackground halts the background worker (unsubscribing from the poke bus and
// cancelling any in-flight re-probe) and joins its goroutine. Safe to call when
// StartBackground was never called.
func (c *ClusterCoreController) StopBackground() {
	if c.bgCancel != nil {
		c.bgCancel()
	}
	c.bgWG.Wait()
}

// Reprobe requests an immediate out-of-band re-probe of one cluster. Non-blocking:
// the request is dropped if the bus buffer is full (a retry is already queued).
// Backs Service.RetryConnection.
func (c *ClusterCoreController) Reprobe(id ClusterID) {
	select {
	case c.retryCh <- id:
	default:
	}
}

// reprobeAll re-runs the connection reconcile for every eligible cluster,
// forcing an immediate probe + health check (refreshing the connMgr config and
// the Connected/Healthy conditions) instead of waiting for the next scheduled
// reconcile. Driven by the poke bus on OS resume / network-on.
func (c *ClusterCoreController) reprobeAll(baseCtx context.Context) {
	objs, err := c.coreClient.List(baseCtx)
	if err != nil {
		if baseCtx.Err() == nil {
			slog.Warn("clustercontroller: poke list clusters", "err", err)
		}
		return
	}
	for _, obj := range objs {
		if baseCtx.Err() != nil {
			return // shutting down
		}
		c.reprobeObj(baseCtx, obj)
	}
}

// reprobeOne re-runs the connection reconcile for one cluster by id, forcing an
// immediate probe out-of-band. Driven by the targeted-retry bus
// (Service.RetryConnection). An unknown id is a no-op (it may have been deleted
// between the mutation and the worker draining the bus).
func (c *ClusterCoreController) reprobeOne(baseCtx context.Context, id ClusterID) {
	obj, err := c.coreClient.GetBySlug(baseCtx, ClusterSlug(id))
	if err != nil {
		if !errors.Is(err, beehive.ErrNotFound) && baseCtx.Err() == nil {
			slog.Warn("clustercontroller: retry get cluster", "cluster", id, "err", err)
		}
		return
	}
	c.reprobeObj(baseCtx, obj)
}

// reprobeObj forces an immediate out-of-band reconcile of one eligible cluster
// object (shared by the poke-driven reprobeAll and the targeted reprobeOne). A
// probe racing a beehive-scheduled reconcile for the same cluster just does a
// redundant probe with an idempotent status write — re-probes are infrequent, so
// the double-probe is acceptable.
func (c *ClusterCoreController) reprobeObj(baseCtx context.Context, obj *beehive.Object[ClusterCoreSpec, ClusterCoreStatus]) {
	if !ConnectionEligible(obj) {
		return
	}
	ctx, cancel := context.WithTimeout(baseCtx, pokeReprobeTimeout)
	defer cancel()
	if _, err := c.Reconcile(ctx, c.ctrlClient, obj); err != nil {
		slog.Warn("clustercontroller: out-of-band re-probe", "cluster", obj.ID, "err", err)
	}
}

// Reconcile converges one Cluster object. The reconcile steps run in sequence;
// the first failure short-circuits (probe failure records an observation and
// requests backoff, store errors return an error for the harness to retry).
func (c *ClusterCoreController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterCoreStatus], obj *beehive.Object[ClusterCoreSpec, ClusterCoreStatus]) (beehive.Result, error) {
	if obj.DeletionRequestedAt != nil {
		// No finalizers to clear; beehive GC handles ClusterCache cascade.
		return beehive.Result{}, nil
	}

	// Extract the cluster UUID from the slug.
	clusterID := clusterIDFromObj(obj)

	// Ensure ClusterCache child exists.
	if err := c.ensureClusterCache(ctx, clusterID, obj.ID); err != nil {
		return beehive.Result{}, err
	}

	// Load (or seed) the working status copy.
	var loaded ClusterCoreStatus
	if obj.Status != nil {
		loaded = *obj.Status
	}
	working := ClusterCoreStatus{
		Source:          loaded.Source,
		Server:          loaded.Server,
		Principal:       loaded.Principal,
		LastConnectedAt: loaded.LastConnectedAt,
		Conditions:      slices.Clone(loaded.Conditions),
	}

	// Mirror SourceObs into status so the GraphQL resolver reads from status.
	if obj.Spec.SourceObs != nil {
		working.Source.Kubeconfig = obj.Spec.SourceObs
	}

	requeueAfter := c.converge(ctx, obj, &working)

	if ClusterCoreStatusEqual(loaded, working) {
		return beehive.Result{RequeueAfter: requeueAfter}, nil
	}
	return beehive.Result{RequeueAfter: requeueAfter},
		client.UpdateStatus(ctx, obj.ID, obj.Generation, working)
}

// converge runs the eligibility gate → credential resolution → probe → health
// phases, recording observations on working, and returns the desired
// RequeueAfter delay.
func (c *ClusterCoreController) converge(ctx context.Context, obj *beehive.Object[ClusterCoreSpec, ClusterCoreStatus], working *ClusterCoreStatus) time.Duration {
	gen := obj.Generation
	conds := &working.Conditions

	clusterID := clusterIDFromObj(obj)

	if !ConnectionEligible(obj) {
		if c.connMgr != nil {
			c.connMgr.Delete(clusterID)
		}
		SetCondition(conds, ClusterCondition{
			Type:               ClusterConditionConnected,
			Status:             ConditionFalse,
			Reason:             ReasonInactive,
			ObservedGeneration: gen,
		})
		SetCondition(conds, ClusterCondition{
			Type:               ClusterConditionHealthy,
			Status:             ConditionUnknown,
			Reason:             ReasonInactive,
			ObservedGeneration: gen,
		})
		return 0
	}

	contextName := obj.Spec.Source.Kubeconfig.Context
	restCfg, err := ResolveRESTConfig(c.cfgSource.Get(), contextName)
	if err != nil {
		if c.connMgr != nil {
			c.connMgr.Delete(clusterID)
		}
		return c.observeConnectFailure(conds, gen, ReasonResolveFailed, err)
	}

	server, principal, err := c.probe(ctx, restCfg)
	if err != nil {
		if c.connMgr != nil {
			c.connMgr.Delete(clusterID)
		}
		return c.observeConnectFailure(conds, gen, ReasonProbeFailed, err)
	}

	if c.connMgr != nil {
		c.connMgr.Set(clusterID, restCfg)
	}

	now := time.Now().UTC()
	working.Server = server
	working.Principal = principal
	working.LastConnectedAt = &now
	SetCondition(conds, ClusterCondition{
		Type:               ClusterConditionConnected,
		Status:             ConditionTrue,
		Reason:             ReasonConnected,
		ObservedGeneration: gen,
	})

	phase, msg := c.check(ctx, restCfg)
	SetCondition(conds, healthCondition(phase, msg, gen))
	return healthProbeInterval
}

// observeConnectFailure records a failed connect attempt on the working
// conditions and returns the doubling-backoff requeue via the beehive harness.
func (c *ClusterCoreController) observeConnectFailure(conds *[]ClusterCondition, gen int64, reason string, err error) time.Duration {
	SetCondition(conds, ClusterCondition{
		Type: ClusterConditionConnected, Status: ConditionFalse,
		Reason: reason, Message: err.Error(), ObservedGeneration: gen,
	})
	SetCondition(conds, ClusterCondition{
		Type: ClusterConditionHealthy, Status: ConditionUnknown,
		Reason: ReasonNoConnection, ObservedGeneration: gen,
	})
	return connectionInitialBackoff
}

// ensureClusterCache creates the ClusterCache child if it does not exist.
func (c *ClusterCoreController) ensureClusterCache(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID) error {
	_, err := c.cacheClient.GetBySlug(ctx, ClusterCacheSlug(clusterID))
	if err == nil {
		return nil // already exists
	}
	if !errors.Is(err, beehive.ErrNotFound) {
		return err
	}
	_, err = c.cacheClient.Create(ctx, ClusterCacheSpec{},
		beehive.WithSlug(ClusterCacheSlug(clusterID)),
		beehive.WithOwner(ownerID),
	)
	return err
}

// clusterIDFromObj extracts the ClusterID UUID from the Cluster object's slug.
func clusterIDFromObj(obj *beehive.Object[ClusterCoreSpec, ClusterCoreStatus]) ClusterID {
	if obj.Slug == nil {
		return ""
	}
	return ClusterIDFromSlug(*obj.Slug)
}

// healthCondition maps one health-probe outcome onto the Healthy condition.
func healthCondition(phase HealthPhase, msg *string, gen int64) ClusterCondition {
	cond := ClusterCondition{Type: ClusterConditionHealthy, ObservedGeneration: gen}
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
