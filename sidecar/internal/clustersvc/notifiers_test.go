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

package clustersvc

import (
	"context"
	"errors"
	"testing"

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// probeSourceClient reports every requeue on a probe, so a test blocks on the poke
// rather than on the clock.
type probeSourceClient struct {
	stubSourceClient
	seen *testutil.Probe[beehive.ObjectID]
}

func (c *probeSourceClient) Requeue(ctx context.Context, id beehive.ObjectID, opts ...beehive.RequeueOption) error {
	err := c.stubSourceClient.Requeue(ctx, id, opts...)
	c.seen.Fire(id)
	return err
}

// startNotifier wires a kubeconfig notifier onto src and joins it on cleanup.
func startNotifier(t *testing.T, src *fakeKubeconfigSource) (*notifier[*api.Config], *probeSourceClient, func(context.Context) error) {
	t.Helper()
	client := &probeSourceClient{
		stubSourceClient: stubSourceClient{id: 7},
		seen:             testutil.NewProbe[beehive.ObjectID](16),
	}

	n := newKubeconfigNotifier(src, client)
	stop, err := n.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
	return n, client, stop
}

// The subscription is current-on-subscribe, so whatever the file already held pokes
// the anchor at startup rather than waiting for the user to edit it.
func TestNotifierPokesForTheStartupSnapshot(t *testing.T) {
	_, client, _ := startNotifier(t, newFakeKubeconfigSource(cfgWith("prod")))

	assert.Equal(t, beehive.ObjectID(7), client.seen.Await(t, "startup poke"))
}

func TestNotifierPokesForEachSnapshot(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, client, _ := startNotifier(t, src)
	client.seen.Await(t, "startup poke")

	src.publish(cfgWith("prod", "staging"))
	client.seen.Await(t, "poke for the second snapshot")
}

// A failed poke is dropped, not retried: the anchor re-arms on its own interval, and
// a ladder here would be a second one to keep in step with beehive's.
func TestNotifierSurvivesAFailedPoke(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	client := &probeSourceClient{
		stubSourceClient: stubSourceClient{id: 7, requeueEr: errors.New("boom")},
		seen:             testutil.NewProbe[beehive.ObjectID](16),
	}
	n := newKubeconfigNotifier(src, client)
	stop, err := n.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	client.seen.Await(t, "startup poke")
	src.publish(cfgWith("staging"))
	client.seen.Await(t, "the loop keeps running past a failure")
}

// An anchor the store cannot hand back must not take the loop down with it.
func TestNotifierSurvivesAFailedLookup(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	client := &probeSourceClient{
		stubSourceClient: stubSourceClient{getErr: errors.New("boom")},
		seen:             testutil.NewProbe[beehive.ObjectID](16),
	}
	n := newKubeconfigNotifier(src, client)
	stop, err := n.Start(context.Background())
	require.NoError(t, err)

	src.publish(cfgWith("staging"))
	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")
	assert.Empty(t, client.stubSourceClient.requeued)
}

// The watcher shutting down ends the loop: nothing else would, and a goroutine parked
// on a closed channel outlives the service.
func TestNotifierStopsWhenTheSourceCloses(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	n, client, _ := startNotifier(t, src)
	client.seen.Await(t, "startup poke")

	src.close()

	// Join the loop directly rather than through the stop func, which also cancels the
	// base context — either exit would then satisfy the assertion. Waiting on the
	// WaitGroup alone leaves the closed channel as the only way out.
	testutil.WaitReturn(t, n.wg.Wait, "loop to end when the source closed")
}

// The stop func must join the loop even mid-poke, so service.Close never races one.
func TestNotifierStopJoinsTheLoop(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	_, client, stop := startNotifier(t, src)
	client.seen.Await(t, "startup poke")

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")
}

// The app owns the kubeconfig service and hands it to every reader, so the notifier
// must not close it: Close ends every subscription the process holds.
func TestNotifierCloseLeavesTheKubeconfigServiceOpen(t *testing.T) {
	d := newTestDeps(t)
	n := newKubeconfigNotifier(d.kubeconfigSvc, d.sourceClient)

	stop, err := n.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
	require.NoError(t, n.Close())

	sub := d.kubeconfigSvc.Subscribe()
	defer sub.Close()
	testutil.Recv(t, sub.Chan(), "the current config from a service still open")
}

// --- the seam ---

// stringFeed is a hand-driven feed carrying a type no source here produces, so the
// loop is exercised against the seam rather than against its one implementation.
type stringFeed struct {
	ch     chan string
	closed *testutil.Signal
}

func (f stringFeed) Chan() <-chan string { return f.ch }
func (f stringFeed) Close()              { f.closed.Fire() }

// A notifier pokes the anchor it was named for, over whatever feed it was given.
func TestNotifierPokesTheAnchorItWasNamedFor(t *testing.T) {
	ch := make(chan string, 1)
	closed := testutil.NewSignal()
	client := &probeSourceClient{
		stubSourceClient: stubSourceClient{id: 9},
		seen:             testutil.NewProbe[beehive.ObjectID](4),
	}

	n := newNotifier("clustersource/other", func() feed[string] { return stringFeed{ch: ch, closed: closed} }, client)
	stop, err := n.Start(context.Background())
	require.NoError(t, err)

	ch <- "changed"
	assert.Equal(t, beehive.ObjectID(9), client.seen.Await(t, "poke"))
	assert.Equal(t, []string{"clustersource/other"}, client.asked)

	// The loop owns the subscription, so stopping it is what releases the feed.
	require.NoError(t, stop(context.Background()))
	testutil.Wait(t, closed.Chan(), "the feed released on exit")
}
