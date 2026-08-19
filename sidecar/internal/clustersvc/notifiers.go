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

// Notifiers: the bridge from a change feed outside beehive to the records a change
// reaches. Each maps a source's own vocabulary onto beehive names and hands the channel
// to a trigger declared at registration — beehive owns the receive loop, its rate
// against the store, and its place in the shutdown order.
//
// Translation is all that is left here, and it is the part beehive cannot do: a source
// of truth is not an object, and only this package knows that the kube-context "prod" is
// the record named "kubeconfig/prod".
package clustersvc

import (
	"context"
	"sync"

	"github.com/amorey/gobus"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeidentity"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// feed is a source's change stream: one value per change, closed when the source stops.
// The value names what moved and nothing more — a poke asks for a pass, and the pass
// reads the source itself, so handing it a snapshot through a queue that coalesces is
// how an observation gets written from a state that has already moved on.
type feed[T any] interface {
	Chan() <-chan T
	Close()
}

// notifier turns one feed into the names a beehive trigger requeues. It holds the
// subscription for as long as the process wants the feed, so it is a lifecycle.Part like
// everything else that runs.
//
// It carries no retry ladder. A lost poke costs latency rather than divergence — every
// kind it pokes re-arms on a cadence of its own, which is what re-reads a source when
// nothing told it to.
type notifier[T any] struct {
	subscribe func() feed[T]
	// name maps one value from the feed onto the beehive name that moved. An address,
	// never state.
	name func(T) string
	// names is what the trigger reads. Unbuffered: beehive floors how often a poke
	// reaches the store, and a queue here would be a second policy about the same thing.
	names chan string

	wg sync.WaitGroup
}

func newNotifier[T any](subscribe func() feed[T], name func(T) string) *notifier[T] {
	return &notifier[T]{subscribe: subscribe, name: name, names: make(chan string)}
}

// Names is the channel to declare as a kind's trigger. Read before Start, since
// registration comes first.
func (n *notifier[T]) Names() <-chan string { return n.names }

// Start subscribes and translates until stopped. The subscription is established before
// Start returns, so a current-on-subscribe feed pokes once for whatever the source
// already held.
func (n *notifier[T]) Start(context.Context) (func(context.Context) error, error) {
	// Not Start's context, which bounds startup: this one bounds the loop and the send in
	// flight, so it lives until the stop func cancels it.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	sub := n.subscribe()
	n.wg.Go(func() {
		// Closing the channel is what ends the trigger reading it, after beehive drains
		// what this loop already sent.
		defer close(n.names)
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

// run forwards one name per change until stopped or the feed closes.
func (n *notifier[T]) run(ctx context.Context, sub feed[T]) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-sub.Chan():
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case n.names <- n.name(v):
			}
		}
	}
}

// --- kubeconfig ---

// kubeconfigSource is the subscribe half of kubeconfigService, all a notifier needs,
// narrow so a test can substitute a hand-driven hub.
type kubeconfigSource interface {
	Subscribe() kubeconfig.Subscription
}

// newKubeconfigNotifier watches the user's kubeconfig for the kubeconfig anchor. Every
// change names the same record: which contexts moved is that pass's to work out.
func newKubeconfigNotifier(cfgSource kubeconfigSource) *notifier[*api.Config] {
	return newNotifier(
		func() feed[*api.Config] { return cfgSource.Subscribe() },
		func(*api.Config) string { return ClusterSourceNameKubeconfig },
	)
}

// --- kubeidentity ---

// kubeidentitySource is the subscribe half of kubeidentityService, all a notifier needs,
// narrow so a test can substitute a hand-driven hub.
type kubeidentitySource interface {
	Subscribe() kubeidentity.Subscription
}

// newKubeidentityNotifier wakes the identity record for a context whose probe answered
// differently. A record reads what is known from kubeidentity rather than from the
// store, so beehive cannot know when that observation went stale; this is the only thing
// that reaches it, and the kind's own resync is what covers a signal that went missing.
func newKubeidentityNotifier(idSource kubeidentitySource) *notifier[gobus.Event[string, struct{}]] {
	return newNotifier(
		func() feed[gobus.Event[string, struct{}]] { return idSource.Subscribe() },
		// The event's key is the context that moved; its value carries nothing, since
		// what it now says is the pass's to read.
		func(ev gobus.Event[string, struct{}]) string { return ClusterIdentityName(ev.Key) },
	)
}
