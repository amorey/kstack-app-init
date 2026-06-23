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
	"sync"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// KubeConfigSource is the read surface of the kubeconfig watcher. Satisfied by
// *k8shelpers.KubeConfigWatcher; tests substitute a static fake.
type KubeConfigSource interface {
	Get() *api.Config
	Subscribe() k8shelpers.KubeConfigSubscription
}

// ConnectionEligible reports whether a Cluster record should have a live
// connection: kubeconfig-sourced, observed present, activated, and not being
// deleted.
func ConnectionEligible(obj *beehive.Object[ClusterSpec, ClusterConnectionStatus]) bool {
	return obj.DeletionRequestedAt == nil &&
		obj.Spec.IsActive &&
		obj.Spec.Source.Kubeconfig != nil &&
		obj.Spec.SourceObs != nil &&
		obj.Spec.SourceObs.IsPresent
}

// ResolveRESTConfig materializes client credentials for one kube-context.
func ResolveRESTConfig(cfg *api.Config, contextName string) (*rest.Config, error) {
	return clientcmd.NewNonInteractiveClientConfig(
		*cfg, contextName, &clientcmd.ConfigOverrides{}, nil,
	).ClientConfig()
}

// pokeSubscription runs a handler on every signal from the poke bus until
// stopped. It owns the subscription, the worker goroutine, and a base context
// cancelled on stop (so a long-running handler is interrupted). Both cluster
// controllers use it to react to resync pokes, driven by their StartPoke/StopPoke
// which the cluster Service sequences around the beehive lifecycle.
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
