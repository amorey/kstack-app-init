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

// ClusterController reconciles Cluster beehive objects. On each pass it:
//  1. Creates a ClusterCache child if one does not exist.
//  2. Mirrors ClusterSpec.SourceObs into ClusterConnectionStatus.Source.Kubeconfig
//     (the kubeconfig observation written by the kubeconfig importer).
//  3. Gates on eligibility (active + kubeconfig present + not being deleted).
//  4. Resolves REST credentials from the current kubeconfig.
//  5. Probes the connection (server version + UID + principal).
//  6. Probes server health (readyz endpoint).
//  7. Writes all observations to ClusterConnectionStatus via UpdateStatus.
//  8. Updates connMgr: Set on success, Delete on failure or ineligibility.
type ClusterController struct {
	cfgSource   KubeConfigSource
	coreClient  beehive.Client[ClusterSpec, ClusterConnectionStatus]
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	connMgr     *ConnectionManager
	ctrlClient  beehive.ControllerClient[ClusterConnectionStatus]

	// pokeSvc is the resync bus; nil disables poke-driven re-probes (tests).
	pokeSvc *poke.Service
	pokeSub *pokeSubscription

	probe ProbeFunc
	check CheckFunc
}

// pokeReprobeTimeout bounds one cluster's poke-driven re-probe (its own probe +
// health round-trips are separately capped inside probe/check).
const pokeReprobeTimeout = connectionProbeTimeout + healthProbeTimeout

// NewClusterController builds the controller. coreClient lets it enumerate
// clusters for a poke-driven re-probe; connMgr may be nil (no connection
// tracking); pokeSvc may be nil (no poke-driven re-probe). probe and check
// default to the real network implementations; tests inject fakes.
func NewClusterController(
	cfgSource KubeConfigSource,
	coreClient beehive.Client[ClusterSpec, ClusterConnectionStatus],
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus],
	connMgr *ConnectionManager,
	pokeSvc *poke.Service,
	probe ProbeFunc,
	check CheckFunc,
) *ClusterController {
	if probe == nil {
		probe = probeCluster
	}
	if check == nil {
		check = checkServerHealth
	}
	return &ClusterController{
		cfgSource:   cfgSource,
		coreClient:  coreClient,
		cacheClient: cacheClient,
		connMgr:     connMgr,
		pokeSvc:     pokeSvc,
		probe:       probe,
		check:       check,
	}
}

// SetControllerClient injects the status-write client obtained from
// beehive.Register. It backs the out-of-band poke re-probe (reprobeAll); the
// reconcile path uses the client beehive passes into Reconcile instead. Call
// once, before the control plane starts.
func (c *ClusterController) SetControllerClient(cl beehive.ControllerClient[ClusterConnectionStatus]) {
	c.ctrlClient = cl
}

// StartPoke subscribes to the resync poke bus, re-probing every eligible cluster
// on each signal. Call after the control plane has started; pair with StopPoke.
func (c *ClusterController) StartPoke() {
	c.pokeSub = startPokeSubscription(c.pokeSvc, c.reprobeAll)
}

// StopPoke halts the poke subscription and joins its goroutine.
func (c *ClusterController) StopPoke() {
	c.pokeSub.stop()
}

// reprobeAll re-runs the connection reconcile for every eligible cluster,
// forcing an immediate probe + health check (refreshing the connMgr config and
// the Connected/Healthy conditions) instead of waiting for the next scheduled
// reconcile. Driven by the poke bus on OS resume / network-on. It reuses
// Reconcile, so a probe racing a beehive-scheduled reconcile for the same
// cluster just does a redundant probe with an idempotent status write — pokes
// are infrequent, so the double-probe is acceptable.
func (c *ClusterController) reprobeAll(baseCtx context.Context) {
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
		if !ConnectionEligible(obj) {
			continue
		}
		ctx, cancel := context.WithTimeout(baseCtx, pokeReprobeTimeout)
		_, err := c.Reconcile(ctx, c.ctrlClient, obj)
		cancel()
		if err != nil {
			slog.Warn("clustercontroller: poke re-probe", "cluster", obj.ID, "err", err)
		}
	}
}

// Reconcile converges one Cluster object. The reconcile steps run in sequence;
// the first failure short-circuits (probe failure records an observation and
// requests backoff, store errors return an error for the harness to retry).
func (c *ClusterController) Reconcile(ctx context.Context, client beehive.ControllerClient[ClusterConnectionStatus], obj *beehive.Object[ClusterSpec, ClusterConnectionStatus]) (beehive.Result, error) {
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
	var loaded ClusterConnectionStatus
	if obj.Status != nil {
		loaded = *obj.Status
	}
	working := ClusterConnectionStatus{
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

	if ClusterConnectionStatusEqual(loaded, working) {
		return beehive.Result{RequeueAfter: requeueAfter}, nil
	}
	return beehive.Result{RequeueAfter: requeueAfter},
		client.UpdateStatus(ctx, obj.ID, obj.Generation, working)
}

// converge runs the eligibility gate → credential resolution → probe → health
// phases, recording observations on working, and returns the desired
// RequeueAfter delay.
func (c *ClusterController) converge(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterConnectionStatus], working *ClusterConnectionStatus) time.Duration {
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
func (c *ClusterController) observeConnectFailure(conds *[]ClusterCondition, gen int64, reason string, err error) time.Duration {
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
func (c *ClusterController) ensureClusterCache(ctx context.Context, clusterID ClusterID, ownerID beehive.ObjectID) error {
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
func clusterIDFromObj(obj *beehive.Object[ClusterSpec, ClusterConnectionStatus]) ClusterID {
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
