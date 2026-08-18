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

// Notifiers: the bridge from a source's own change feed to its ClusterSource anchor,
// one per source that has a feed worth watching. See clustersources.go for the anchors
// they poke.
package clustersvc

import (
	"context"
	"log/slog"
	"sync"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// feed is a source's change stream: one value per change, closed when the source
// stops. The value is dropped — a poke asks for a pass, and the pass reads the source
// itself, so handing it a snapshot through a queue that coalesces is how an
// observation gets written from a state that has already moved on.
type feed[T any] interface {
	Chan() <-chan T
	Close()
}

// notifier requeues one anchor for every change on one feed. It is the only part of
// this subsystem that runs outside beehive, and only because a source of truth is not
// an object: a change to one reaches no record on its own, and there is no dependency
// edge that would span it.
//
// It carries no retry ladder. A lost poke costs latency rather than divergence — the
// anchor re-arms on clusterSourceResyncInterval, which is what re-reads a source when
// nothing told it to.
type notifier[T any] struct {
	// sourceName is the anchor to poke, resolved per poke rather than held: the
	// notifier outlives no record, and a cached id would be a second thing to keep
	// true across a store the bootstrap may have recreated.
	sourceName   string
	subscribe    func() feed[T]
	sourceClient beehive.Client[ClusterSourceSpec, ClusterSourceStatus]

	wg sync.WaitGroup
}

func newNotifier[T any](
	sourceName string,
	subscribe func() feed[T],
	sourceClient beehive.Client[ClusterSourceSpec, ClusterSourceStatus],
) *notifier[T] {
	return &notifier[T]{sourceName: sourceName, subscribe: subscribe, sourceClient: sourceClient}
}

// Start launches the loop and returns the func that ends it. The subscription is
// established before Start returns; a current-on-subscribe feed therefore pokes the
// anchor once for whatever the source already held. Nothing here can fail.
func (n *notifier[T]) Start(context.Context) (func(context.Context) error, error) {
	// Not Start's context, which bounds startup: this one bounds the loop and the poke
	// in flight, so it lives until the stop func cancels it.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	sub := n.subscribe()
	n.wg.Go(func() {
		defer sub.Close()
		n.run(loopCtx, sub)
	})

	return func(ctx context.Context) error {
		stopLoop()
		return drain.WithContext(ctx, n.wg.Wait)
	}, nil
}

// Close is a no-op: the loop releases its subscription as it exits.
func (n *notifier[T]) Close() error { return nil }

// run pokes the anchor for every change until stopped or the feed closes.
func (n *notifier[T]) run(ctx context.Context, sub feed[T]) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sub.Chan():
			if !ok {
				return
			}
			n.poke(ctx)
		}
	}
}

// poke asks for a discovery pass. Every failure is logged and dropped, including a
// missing anchor: the bootstrap creates it before this starts, so absence means a
// store that is already reporting failures of its own.
func (n *notifier[T]) poke(ctx context.Context) {
	obj, err := n.sourceClient.GetByName(ctx, n.sourceName)
	if err == nil {
		err = n.sourceClient.Requeue(ctx, obj.ID)
	}
	// Cancelled with the loop: the error describes the shutdown, not the poke.
	if err != nil && ctx.Err() == nil {
		slog.Error("could not poke cluster source", "name", n.sourceName, "err", err)
	}
}

// --- kubeconfig ---

// kubeconfigSource is the subscribe half of kubeconfigService, all a notifier needs,
// narrow so a test can substitute a hand-driven hub.
type kubeconfigSource interface {
	Subscribe() kubeconfig.Subscription
}

// newKubeconfigNotifier watches the user's kubeconfig for the kubeconfig anchor.
func newKubeconfigNotifier(
	cfgSource kubeconfigSource,
	sourceClient beehive.Client[ClusterSourceSpec, ClusterSourceStatus],
) *notifier[*api.Config] {
	return newNotifier(
		ClusterSourceNameKubeconfig,
		func() feed[*api.Config] { return cfgSource.Subscribe() },
		sourceClient,
	)
}
