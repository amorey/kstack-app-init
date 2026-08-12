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

// Helpers used only by the controllers: the eligibility rules, the owner-chain climb,
// the per-pass condition accumulator, and the kube client/env plumbing a reconcile needs.
package controllers

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"

	"github.com/amorey/beehive"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
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
func ConnectionEligible(obj *beehive.Object[domain.ClusterSpec, domain.ClusterStatus]) bool {
	present := obj.Status != nil &&
		obj.Status.Source.Kubeconfig != nil &&
		obj.Status.Source.Kubeconfig.IsPresent
	return connectionEligible(obj, present)
}

// connectionEligible is ConnectionEligible with presence supplied explicitly.
func connectionEligible(obj *beehive.Object[domain.ClusterSpec, domain.ClusterStatus], present bool) bool {
	return obj.DeletionRequestedAt == nil &&
		obj.Spec.Enabled &&
		obj.Spec.Source.Kubeconfig != nil &&
		present
}

// ownerReader is all resolveCacheChain needs of its starting client. Both beehive.Client and
// ControllerClient satisfy it, which lets the climb start from either — the GVR sync starts
// one hop lower, through its discovery anchor's client.
type ownerReader interface {
	GetOwner(ctx context.Context, id beehive.ObjectID) (beehive.ObjectRef, bool, error)
}

// resolveCacheChain climbs a cache sync child's owner chain — the child → its
// ClusterCache (which names the on-disk cache file) → the Cluster (whose id keys the
// credentials in the connection manager) — returning the on-disk cache locator and the
// Cluster's ObjectID. Every child of a ClusterCache needs exactly this, so the climb and
// its policy live here once rather than in each child's controller.
//
// **A zero cluster id with a nil error means the chain is broken** — an owner already
// collected, which happens while a delete cascades. That is not an error: the caller is
// being cleaned up too, so it should stop rather than retry.
func resolveCacheChain(
	ctx context.Context,
	client ownerReader,
	cacheClient beehive.Client[domain.ClusterCacheSpec, domain.ClusterCacheStatus],
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
	return domain.NewCacheRef(clusterRef.ID, cacheRef.ID), clusterRef.ID, nil
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
	conds ...domain.Condition,
) error {
	// No truncation here: LiveCondition caps every condition this package builds, covering
	// the controllers that write through SetConditions directly too.
	return client.Within(ctx, func(ctx context.Context) error {
		if err := client.SetConditions(ctx, objID, conds); err != nil {
			return err
		}
		return client.SetObservedGeneration(ctx, objID, generation)
	})
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

// conditionSet accumulates one reconcile pass's conditions, upserting by type;
// the whole set is written once via SetConditions under a single version bump, so
// a watcher never sees half a pass.
type conditionSet []domain.Condition

func (s *conditionSet) set(c domain.Condition) {
	for i := range *s {
		if (*s)[i].Type == c.Type {
			(*s)[i] = c
			return
		}
	}
	*s = append(*s, c)
}

// eventsSyncSpec is the Event kind's sync spec — one home, because the discovery
// pass and the pre-pass seed both produce it and must not disagree.
func eventsSyncSpec(enabled bool) domain.ClusterCacheGVRSyncSpec {
	return domain.ClusterCacheGVRSyncSpec{
		Enabled:    enabled,
		APIVersion: domain.EventsAPIVersion,
		Kind:       domain.EventsKind,
		Resource:   domain.EventsResource,
		Namespaced: true,
	}
}

// --- Status equality (the core controller's skip-the-write guard) ---

// ClusterStatusEqual reports whether two ClusterStatus blocks are observably
// equal — the ClusterCoreController's skip-the-write guard.
func ClusterStatusEqual(a, b domain.ClusterStatus) bool {
	return ptrEqual(a.Source.Kubeconfig, b.Source.Kubeconfig) &&
		ptrEqual(a.Server.UID, b.Server.UID) &&
		ptrEqual(a.Server.Version, b.Server.Version) &&
		ptrEqual(a.Principal.Username, b.Principal.Username) &&
		domain.TimePtrEqual(a.LastConnectedAt, b.LastConnectedAt)
}

func ptrEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// send delivers one value on out, honoring ctx cancellation. Returns false if ctx
// ended before the send completed.
func send[T any](ctx context.Context, out chan<- T, v T) bool {
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
}
