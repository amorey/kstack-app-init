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
	"os"
	"strconv"
	"sync"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

const (
	// http2ReadIdleTimeoutSeconds and http2PingTimeoutSeconds tighten client-go's
	// HTTP/2 connection health check, which detects and tears down a silently-dead
	// API-server connection: an idle HTTP/2 connection is pinged after the read-idle
	// timeout, and dropped if no pong arrives within the ping timeout. client-go
	// enables the check by default but at 30s/15s — so a broken watch stream can
	// linger ~45s before anything notices. Tightening to 10s/5s cuts worst-case
	// connection-loss detection to ~15s, which is what lets the connection
	// controller's liveness sentinel see its watch close (→ re-probe) promptly
	// instead of waiting on the health-poll cadence.
	http2ReadIdleTimeoutSeconds = 10
	http2PingTimeoutSeconds     = 5
)

// ConfigureKubeHTTP2Keepalive tightens the HTTP/2 keepalive client-go applies to
// every kube connection, by setting the env vars apimachinery's transport
// defaults read (HTTP2_READ_IDLE_TIMEOUT_SECONDS / HTTP2_PING_TIMEOUT_SECONDS).
// It writes a var only when unset, so an operator (or test) override wins, and is
// safe to call once at startup — before any kube client builds its transport, the
// values are read lazily per transport build. The composition root calls it.
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

// ConnectionEligible reports whether a Cluster record should have a live
// connection: kubeconfig-sourced, observed present, enabled, and not being
// deleted. Presence is read from the observed status (written by the
// ClusterCoreController from the live kubeconfig), so callers that react to a
// record — the cache controller, the targeted-retry pre-gate — see the latest
// committed observation. The ClusterCoreController's own reconcile uses
// connectionEligible with a freshly-observed presence instead, so its gate does
// not lag the status it is about to write.
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
// definition of "active cache" shared by the cache controller (engine gating) and
// the service (domain join + active-cache resolution).
func cacheIsActive(clusterObj *beehive.Object[ClusterSpec, ClusterStatus], cacheUID string) bool {
	active := clusterActiveUID(clusterObj)
	return active != "" && cacheUID == active
}

// ResolveRESTConfig materializes client credentials for one kube-context.
func ResolveRESTConfig(cfg *api.Config, contextName string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveClientConfig(
		*cfg, contextName, &clientcmd.ConfigOverrides{}, nil,
	).ClientConfig()
}

// pokeSubscription runs a handler on every signal from the poke bus until
// stopped. It owns the subscription, the worker goroutine, and a base context
// cancelled on stop (so a long-running handler is interrupted). The cache
// controller uses it (StartPoke/StopPoke) for its single poke-driven reaction;
// the core controller folds the poke bus into its own multi-source background
// worker instead (StartBackground), so it doesn't use this helper.
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
