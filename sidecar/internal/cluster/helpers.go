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
	"os"
	"strconv"
	"sync"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

const (
	// Tighten client-go's HTTP/2 health check from its 30s/15s default, which lets a broken
	// watch linger ~45s. At 10s/5s the sentinel's watch closes (→ re-probe) in ~15s.
	// See docs/adr/2026-08-09-connection-probing.md.
	http2ReadIdleTimeoutSeconds = 10
	http2PingTimeoutSeconds     = 5
)

// ConfigureKubeHTTP2Keepalive sets the env vars apimachinery's transport defaults read,
// only when unset so an operator or test override wins. Call once at startup (the
// composition root does); the values are read lazily per transport build.
func ConfigureKubeHTTP2Keepalive() {
	setEnvIfUnset("HTTP2_READ_IDLE_TIMEOUT_SECONDS", strconv.Itoa(http2ReadIdleTimeoutSeconds))
	setEnvIfUnset("HTTP2_PING_TIMEOUT_SECONDS", strconv.Itoa(http2PingTimeoutSeconds))
}

// setEnvIfUnset preserves an existing override.
func setEnvIfUnset(key, val string) {
	if _, ok := os.LookupEnv(key); !ok {
		_ = os.Setenv(key, val)
	}
}

// KubeConfigSource is the read surface of the kubeconfig watcher. Satisfied by
// *k8shelpers.KubeConfigWatcher; tests substitute a static fake.
type KubeConfigSource interface {
	Get() *api.Config
	Subscribe() k8shelpers.KubeConfigSubscription
}

// ConnectionEligible reports whether a Cluster should have a live connection:
// kubeconfig-sourced, observed present, enabled, not deleting. Presence comes from the
// committed status, so reacting callers see the latest observation; the core controller's
// own reconcile uses connectionEligible with freshly-observed presence instead.
func ConnectionEligible(obj *beehive.Object[ClusterSpec, ClusterStatus]) bool {
	present := obj.Status != nil &&
		obj.Status.Source.Kubeconfig != nil &&
		obj.Status.Source.Kubeconfig.IsPresent
	return connectionEligible(obj, present)
}

// connectionEligible is ConnectionEligible with presence supplied explicitly.
func connectionEligible(obj *beehive.Object[ClusterSpec, ClusterStatus], present bool) bool {
	return obj.DeletionRequestedAt == nil &&
		obj.Spec.Enabled &&
		obj.Spec.Source.Kubeconfig != nil &&
		present
}

// clusterActiveUID returns the last-probed kube-system UID, or "" if never probed. It
// selects which owned ClusterCache is active.
func clusterActiveUID(obj *beehive.Object[ClusterSpec, ClusterStatus]) string {
	if obj.Status != nil && obj.Status.Server.UID != nil {
		return *obj.Status.Server.UID
	}
	return ""
}

// cacheIsActive reports whether a cache mirrors its parent's currently-active identity; one
// for an unknown identity never is. The single definition of "active cache", shared by the
// cache controller's sync gating and the service's domain join.
func cacheIsActive(clusterObj *beehive.Object[ClusterSpec, ClusterStatus], cacheUID string) bool {
	active := clusterActiveUID(clusterObj)
	return active != "" && cacheUID == active
}

// ownerReader is all resolveCacheChain needs of its starting client. Both beehive.Client and
// ControllerClient satisfy it, which lets the climb start from either — the GVR sync starts
// one hop lower, through its discovery anchor's client.
type ownerReader interface {
	GetOwner(ctx context.Context, id beehive.ObjectID) (beehive.ObjectRef, bool, error)
}

// resolveCacheChain climbs a cache sync child's owner chain — the child → its
// ClusterCache (which names the on-disk cache file) → the Cluster (whose id keys the
// credentials in the ConnectionManager) — returning the on-disk cache locator and the
// Cluster's ObjectID. Every child of a ClusterCache needs exactly this, so the climb and
// its policy live here once rather than in each child's controller.
//
// **A zero cluster id with a nil error means the chain is broken** — an owner already
// collected, which happens while a delete cascades. That is not an error: the caller is
// being cleaned up too, so it should stop rather than retry.
func resolveCacheChain(
	ctx context.Context,
	client ownerReader,
	cacheClient beehive.Client[ClusterCacheSpec, ClusterCacheStatus],
	objID beehive.ObjectID,
) (store.CacheRef, beehive.ObjectID, error) {
	cacheRef, ok, err := client.GetOwner(ctx, objID)
	if err != nil || !ok {
		return store.CacheRef{}, 0, err
	}
	clusterRef, ok, err := cacheClient.GetOwner(ctx, cacheRef.ID)
	if err != nil {
		if errors.Is(err, beehive.ErrNotFound) {
			return store.CacheRef{}, 0, nil
		}
		return store.CacheRef{}, 0, err
	}
	if !ok {
		return store.CacheRef{}, 0, nil
	}
	return newCacheRef(clusterRef.ID, cacheRef.ID), clusterRef.ID, nil
}

// maxMessageLen caps a persisted message (on a condition or an event run). Both are read
// back on every frame of a whole-fleet watch, and their sources are unbounded: a raw
// client-go error, or a /readyz?verbose=true body, which routinely runs to kilobytes.
const maxMessageLen = 200

// truncateMessage caps s at maxMessageLen bytes, appending an ellipsis when it overflows.
// (Byte-bounded; error strings are effectively ASCII.)
func truncateMessage(s string) string {
	if len(s) <= maxMessageLen {
		return s
	}
	return s[:maxMessageLen] + "…"
}

// reportCondition records a pass whose whole output is its conditions — no status write,
// because it observed nothing to store or the kind keeps gauges out of status. Variadic
// since a pass may report several axes, which must land together.
//
// **It settles the generation explicitly**, which is the reason this helper exists: a
// condition write does not advance beehive's handshake, so without SetObservedGeneration the
// object stays owed forever. One transaction, so a watcher never sees half a pass.
// See docs/adr/2026-08-09-liveness-conditions.md.
func reportCondition[Status any](
	ctx context.Context,
	client beehive.ControllerClient[Status],
	objID beehive.ObjectID,
	generation int64,
	conds ...Condition,
) error {
	// No truncation here: liveCondition caps every condition this package builds, covering
	// the controllers that write through SetConditions directly too.
	return client.Within(ctx, func(ctx context.Context) error {
		if err := client.SetConditions(ctx, objID, conds); err != nil {
			return err
		}
		return client.SetObservedGeneration(ctx, objID, generation)
	})
}

// ownerObjectID reads an owner id off the eager-loaded owner edge, or 0 when there is none.
// Best-effort: a hard-deleted child has no edge, but the client already dropped it on the
// soft-delete change, and consumers key removal on the object's own id.
//
// Reads the edge the watch loaded with WithLoads(LoadOwner()), resolved once per batch — a
// per-object GetOwner would be an N+1 per frame per subscriber against an edge written once
// at creation, which is why every domain builder goes through this.
func ownerObjectID[Spec, Status any](obj *beehive.Object[Spec, Status]) ObjectID {
	owner, ok, err := obj.Owner()
	if err != nil || !ok {
		return 0
	}
	return ObjectID(owner.ID)
}

// derefOrZero returns *p, or the zero value when nil. beehive leaves Status nil until first
// written, while domain records serve it by value — absent and zeroed say the same thing.
func derefOrZero[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// ResolveRESTConfig materializes client credentials for one kube-context.
func ResolveRESTConfig(cfg *api.Config, contextName string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveClientConfig(
		*cfg, contextName, &clientcmd.ConfigOverrides{}, nil,
	).ClientConfig()
}

// pokeSubscription runs a handler on every poke-bus signal until stopped. It owns the
// subscription, the worker goroutine, and a base context cancelled on stop (so a
// long-running handler is interrupted). The per-kind sync controller uses it (StartPoke/
// StopPoke) to restart its live workers; the core controller folds the poke bus into its
// own multi-source worker instead, so it doesn't.
type pokeSubscription struct {
	cancel func()
	wg     sync.WaitGroup
}

// startPokeSubscription subscribes to pokeSvc and invokes handler(ctx) on each
// signal; ctx is cancelled when the subscription is stopped. Returns nil when
// pokeSvc is nil (poke-driven behavior disabled, e.g. in tests) — stop is
// nil-safe.
func startPokeSubscription(pokeSvc *poke.Service, handler func(context.Context)) *pokeSubscription {
	if pokeSvc == nil {
		return nil
	}
	ch, cancelSub := pokeSvc.Subscribe()
	ctx, cancelCtx := context.WithCancel(context.Background())
	s := &pokeSubscription{cancel: func() { cancelSub(); cancelCtx() }}
	s.wg.Go(func() {
		for range ch {
			handler(ctx)
		}
	})
	return s
}

// stop halts the subscription (closing the channel so the worker returns) and
// joins it. Safe to call on a nil subscription.
func (s *pokeSubscription) stop() {
	if s == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}
