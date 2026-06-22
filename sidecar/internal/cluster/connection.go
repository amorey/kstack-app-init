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
	"slices"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"
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
//     (the kubeconfig observation written by the ClusterSourceController).
//  3. Gates on eligibility (active + kubeconfig present + not being deleted).
//  4. Resolves REST credentials from the current kubeconfig.
//  5. Probes the connection (server version + UID + principal).
//  6. Probes server health (readyz endpoint).
//  7. Writes all observations to ClusterConnectionStatus via UpdateStatus.
type ClusterController struct {
	cfgSource   KubeConfigSource
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus]
	ctrlClient  beehive.ControllerClient[ClusterConnectionStatus]

	probe ProbeFunc
	check CheckFunc
}

// NewClusterController builds the controller. probe and check default to the
// real network implementations; tests inject fakes.
func NewClusterController(
	cfgSource KubeConfigSource,
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus],
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
		cacheClient: cacheClient,
		probe:       probe,
		check:       check,
	}
}

// Start stores the ControllerClient handed in by beehive.
func (c *ClusterController) Start(cl beehive.ControllerClient[ClusterConnectionStatus]) error {
	c.ctrlClient = cl
	return nil
}

// Stop is a no-op — no background goroutines.
func (c *ClusterController) Stop(_ context.Context) error { return nil }

// Reconcile converges one Cluster object. The reconcile steps run in sequence;
// the first failure short-circuits (probe failure records an observation and
// requests backoff, store errors return an error for the harness to retry).
func (c *ClusterController) Reconcile(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterConnectionStatus]) (beehive.Result, error) {
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
		c.ctrlClient.UpdateStatus(ctx, obj.ID, obj.Generation, working)
}

// converge runs the eligibility gate → credential resolution → probe → health
// phases, recording observations on working, and returns the desired
// RequeueAfter delay.
func (c *ClusterController) converge(ctx context.Context, obj *beehive.Object[ClusterSpec, ClusterConnectionStatus], working *ClusterConnectionStatus) time.Duration {
	gen := obj.Generation
	conds := &working.Conditions

	if !ConnectionEligible(obj) {
		SetCondition(conds, ClusterCondition{
			Type: ClusterConditionConnected, Status: ConditionFalse,
			Reason: ReasonInactive, ObservedGeneration: gen,
		})
		SetCondition(conds, ClusterCondition{
			Type: ClusterConditionHealthy, Status: ConditionUnknown,
			Reason: ReasonInactive, ObservedGeneration: gen,
		})
		return 0
	}

	contextName := obj.Spec.Source.Kubeconfig.Context
	restCfg, err := ResolveRESTConfig(c.cfgSource.Get(), contextName)
	if err != nil {
		return c.observeConnectFailure(conds, gen, ReasonResolveFailed, err)
	}

	server, principal, err := c.probe(ctx, restCfg)
	if err != nil {
		return c.observeConnectFailure(conds, gen, ReasonProbeFailed, err)
	}

	now := time.Now().UTC()
	working.Server = server
	working.Principal = principal
	working.LastConnectedAt = &now
	SetCondition(conds, ClusterCondition{
		Type: ClusterConditionConnected, Status: ConditionTrue,
		Reason: ReasonConnected, ObservedGeneration: gen,
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
