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

package controllers

import (
	"errors"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/connections"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// controllerRuntime is the shared controller environment (beehive's Manager
// analogue): controller constructors take one *controllerRuntime plus their own
// specifics. It holds the beehive instance, not per-kind clients — each controller
// mints the typed clients it needs from rt.bh, keeping its kinds explicit. Any
// field may be nil in a test that doesn't exercise it.
type controllerRuntime struct {
	bh           *beehive.Beehive
	connMgr      *connections.Manager
	cacheManager *store.Manager
	pokeSvc      *poke.Service
	// cachePolicies is the per-cache client budget (rate limiter + LIST semaphore),
	// shared by the sync and discovery controllers; lazily created so a bare test
	// runtime still gets one.
	cachePolicies *cacheClientPolicies
}

// policies returns the runtime's per-cache client budgets, created on first use
// so every controller built from one runtime shares them.
func (rt *controllerRuntime) policies() *cacheClientPolicies {
	if rt.cachePolicies == nil {
		rt.cachePolicies = newCacheClientPolicies()
	}
	return rt.cachePolicies
}

// Set is the controller handles the caller keeps: the background lifecycle it
// starts and drains, the in-memory gauges it serves, and the worker restart that
// backs a cache clear.
type Set struct {
	Core      *ClusterCoreController
	Discovery *ClusterCacheGVRDiscoveryController
	Sync      *ClusterCacheGVRSyncController
}

// Install builds the four controllers over one shared runtime and registers each
// with beehive. Registration options live here, beside the controllers they
// configure, so the caller never has to know a kind's concurrency or retry policy.
//
// Register returns each kind's status-write ControllerClient; it is injected before
// the caller starts beehive (a startup reconcile may write immediately) since
// controllers also write status out-of-band. WithMaxRetryInterval caps the
// reconnect backoff.
//
// WithStartupFullPass is per kind: each of the four owns process-scoped state a
// restart invalidates that the store never recorded (connections + sentinels,
// running workers, in-memory RequeueAfter re-discovery), so beehive's owed pass
// alone would leave settled objects unreconciled. See
// docs/adr/2026-08-09-beehive-control-plane.md.
func Install(
	bh *beehive.Beehive,
	cfgSource KubeConfigSource,
	cacheManager *store.Manager,
	connMgr *connections.Manager,
	pokeSvc *poke.Service,
) (*Set, error) {
	rt := &controllerRuntime{bh: bh, connMgr: connMgr, cacheManager: cacheManager, pokeSvc: pokeSvc}

	coreCtrl := NewClusterCoreController(rt, cfgSource, nil, nil)
	cacheCtrl := NewClusterCacheController(rt)
	// GVR discovery creates one ClusterCacheGVRSync per served kind (Events included);
	// the sync controller runs one worker per record. Pausing is Spec.Enabled, never
	// a deletion; a worker is only ever started/stopped by its own controller.
	discoveryCtrl := NewClusterCacheGVRDiscoveryController(rt)
	syncCtrl := NewClusterCacheGVRSyncController(rt)

	// WithConcurrency: a Cluster reconcile is mostly one network probe, so a single
	// worker would serialize every cluster behind one dial timeout; per-cluster
	// locks make concurrent reconciles of distinct clusters safe.
	coreCC, errCluster := beehive.Register(bh, domain.ClusterGroupKind, coreCtrl,
		beehive.WithMaxRetryInterval(connectionMaxBackoff),
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(clusterProbeConcurrency),
	)
	_, errCache := beehive.Register(bh, domain.ClusterCacheGroupKind, cacheCtrl,
		beehive.WithStartupFullPass(true),
	)
	// Same concurrency rationale as Cluster: a pass is mostly one discovery request.
	_, errDiscovery := beehive.Register(bh, domain.ClusterCacheGVRDiscoveryGroupKind, discoveryCtrl,
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(clusterProbeConcurrency),
	)
	// Here the concurrency is about volume: hundreds of per-kind records per cache
	// on every startup pass.
	gvrSyncCC, errGVRSync := beehive.Register(bh, domain.ClusterCacheGVRSyncGroupKind, syncCtrl,
		beehive.WithStartupFullPass(true),
		beehive.WithConcurrency(gvrSyncConcurrency),
	)
	if err := errors.Join(errCluster, errCache, errDiscovery, errGVRSync); err != nil {
		return nil, err
	}
	coreCtrl.SetControllerClient(coreCC)
	syncCtrl.SetControllerClient(gvrSyncCC)

	return &Set{Core: coreCtrl, Discovery: discoveryCtrl, Sync: syncCtrl}, nil
}
