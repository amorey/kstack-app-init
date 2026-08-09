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
	// http2ReadIdleTimeoutSeconds and http2PingTimeoutSeconds tighten client-go's
	// HTTP/2 health check, which pings an idle connection after the read-idle timeout
	// and drops it if no pong arrives within the ping timeout. client-go's default
	// 30s/15s lets a broken watch stream linger ~45s; tightening to 10s/5s cuts
	// worst-case connection-loss detection to ~15s, so the liveness sentinel sees its
	// watch close (→ re-probe) promptly instead of waiting on the health-poll cadence.
	http2ReadIdleTimeoutSeconds = 10
	http2PingTimeoutSeconds     = 5
)

// ConfigureKubeHTTP2Keepalive tightens the HTTP/2 keepalive client-go applies to
// every kube connection, by setting the env vars apimachinery's transport defaults
// read (HTTP2_READ_IDLE_TIMEOUT_SECONDS / HTTP2_PING_TIMEOUT_SECONDS). It writes a
// var only when unset, so an operator (or test) override wins. Call once at startup;
// the values are read lazily per transport build. The composition root calls it.
func ConfigureKubeHTTP2Keepalive() {
	setEnvIfUnset("HTTP2_READ_IDLE_TIMEOUT_SECONDS", strconv.Itoa(http2ReadIdleTimeoutSeconds))
	setEnvIfUnset("HTTP2_PING_TIMEOUT_SECONDS", strconv.Itoa(http2PingTimeoutSeconds))
}

// setEnvIfUnset sets key=val only when key is currently unset, so an existing
// override is preserved.
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

// ConnectionEligible reports whether a Cluster record should have a live connection:
// kubeconfig-sourced, observed present, enabled, and not being deleted. Presence is
// read from the observed status (written by the ClusterCoreController), so callers
// that react to a record — the cache controller, the targeted-retry pre-gate — see
// the latest committed observation. The controller's own reconcile uses
// connectionEligible with a freshly-observed presence, so its gate doesn't lag the
// status it's about to write.
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

// clusterActiveUID returns the kube-system UID of a cluster's currently-connected
// physical identity — its last-probed Server.UID — or "" if it has never
// successfully probed. This UID selects which owned ClusterCache is active.
func clusterActiveUID(obj *beehive.Object[ClusterSpec, ClusterStatus]) string {
	if obj.Status != nil && obj.Status.Server.UID != nil {
		return *obj.Status.Server.UID
	}
	return ""
}

// cacheIsActive reports whether a ClusterCache mirroring kube-system UID cacheUID
// is its parent's currently-active identity (cacheUID == the cluster's active UID).
// A cache for an empty/unknown identity is never active. This is the single
// definition of "active cache" shared by the cache controller (sync gating) and
// the service (domain join + active-cache resolution).
func cacheIsActive(clusterObj *beehive.Object[ClusterSpec, ClusterStatus], cacheUID string) bool {
	active := clusterActiveUID(clusterObj)
	return active != "" && cacheUID == active
}

// ownerReader is the one method resolveCacheChain needs of its starting client. Both
// beehive.Client and beehive.ControllerClient satisfy it, which is what lets the climb
// start from either a controller's own reconcile client or a plain kind client (the GVR
// sync starts one hop further down and climbs through its discovery anchor's).
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

// reportCondition records a pass's conditions as a controller's whole report — no status
// write, either because the pass observed nothing to store (a paused child, one waiting on
// credentials, a failed probe) or because the kind keeps its gauges out of status
// entirely. Variadic because a pass may report on several axes at once, and they must land
// together.
//
// **It settles the generation explicitly**, which is the whole reason this is a helper: a
// condition write does not advance beehive's handshake, so without the SetObservedGeneration
// the object would sit unsettled and be re-enqueued by the owed pass forever. The two writes
// land in one transaction so a watcher never sees half a pass, and folding into the object's
// existing conditions keeps LastTransitionTime — and writes nothing at all — when the state
// is unchanged, which is what makes a steady-state pass free.
// maxMessageLen caps a message this package persists — on a condition or on an event run.
// Both are read back on every frame of a whole-fleet watch, and their sources are
// unbounded: a raw client-go error, or the body of a /readyz?verbose=true response, which
// lists every check and routinely runs to kilobytes.
const maxMessageLen = 200

// truncateMessage caps s at maxMessageLen bytes, appending an ellipsis when it overflows.
// (Byte-bounded; error strings are effectively ASCII.)
func truncateMessage(s string) string {
	if len(s) <= maxMessageLen {
		return s
	}
	return s[:maxMessageLen] + "…"
}

func reportCondition[Status any](
	ctx context.Context,
	client beehive.ControllerClient[Status],
	objID beehive.ObjectID,
	generation int64,
	conds ...Condition,
) error {
	// No truncation here: liveCondition caps every condition this package builds, which is
	// what also covers the controllers that write conditions through SetConditions directly.
	return client.Within(ctx, func(ctx context.Context) error {
		if err := client.SetConditions(ctx, objID, conds); err != nil {
			return err
		}
		return client.SetObservedGeneration(ctx, objID, generation)
	})
}

// ownerObjectID reads an object's owner id off its eager-loaded owner edge, or 0 when it
// has none. Best-effort by design: a hard-deleted child has no edge, but by then the
// client has already dropped it on the soft-delete → Deleted change, so a zero id on the
// trailing hard Deleted is harmless — consumers key removal on the object's own id.
//
// It reads the edge the watch loaded with WithLoads(LoadOwner()), which beehive resolves
// once per batch. A per-object GetOwner here would be an N+1 against an edge written at
// creation and never rewritten, re-run for every frame and every subscriber — which is
// why every domain builder goes through this rather than calling the client.
func ownerObjectID[Spec, Status any](obj *beehive.Object[Spec, Status]) ObjectID {
	owner, ok, err := obj.Owner()
	if err != nil || !ok {
		return 0
	}
	return ObjectID(owner.ID)
}

// derefOrZero returns *p, or the zero value when p is nil. beehive leaves Status nil until
// a controller first writes it, while the domain records serve status by value — an absent
// status and a zeroed one are the same statement to a consumer.
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
