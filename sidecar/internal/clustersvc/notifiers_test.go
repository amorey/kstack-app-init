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
	"testing"

	"github.com/amorey/gobus/conflate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeidentity"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// startNotifier starts n and joins it on cleanup.
func startNotifier[T any](t *testing.T, n *notifier[T]) {
	t.Helper()
	stop, err := n.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
}

// The subscription is current-on-subscribe, so whatever the file already held names the
// anchor at startup rather than waiting for the user to edit it.
func TestKubeconfigNotifierNamesTheAnchorForTheStartupSnapshot(t *testing.T) {
	n := newKubeconfigNotifier(newFakeKubeconfigSource(cfgWith("prod")))
	startNotifier(t, n)

	assert.Equal(t, ClusterSourceNameKubeconfig, testutil.Recv(t, n.Names(), "the startup poke"))
}

// Every change names the same record: which contexts moved is the anchor's pass to work
// out, so the notifier translates rather than deciding.
func TestKubeconfigNotifierNamesTheAnchorForEachSnapshot(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	n := newKubeconfigNotifier(src)
	startNotifier(t, n)
	testutil.Recv(t, n.Names(), "the startup poke")

	src.publish(cfgWith("prod", "staging"))

	assert.Equal(t, ClusterSourceNameKubeconfig, testutil.Recv(t, n.Names(), "the second poke"))
}

// The context that answered is the record that moved, and mapping the two is the one
// thing beehive cannot do: only this package knows the record's name.
func TestKubeidentityNotifierNamesTheContextsRecord(t *testing.T) {
	src := newFakeIdentitySource()
	n := newKubeidentityNotifier(src)
	startNotifier(t, n)

	src.publish("staging")

	assert.Equal(t, ClusterIdentityName("staging"), testutil.Recv(t, n.Names(), "the poke"))
}

// The watcher shutting down ends the loop, and closing the channel is what ends the
// trigger reading it — beehive drains what was already sent and stops.
func TestNotifierClosesItsChannelWhenTheSourceCloses(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	n := newKubeconfigNotifier(src)
	startNotifier(t, n)
	testutil.Recv(t, n.Names(), "the startup poke")

	src.close()

	testutil.WaitClosed(t, n.Names(), "the names channel to close with the feed")
}

// The stop func must join the loop even mid-send, so service.Close never races one.
// Nothing reads Names here, so the loop is parked on exactly that send.
func TestNotifierStopJoinsTheLoop(t *testing.T) {
	n := newKubeconfigNotifier(newFakeKubeconfigSource(cfgWith("prod")))
	stop, err := n.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")
}

// The app owns the kubeconfig service and hands it to every reader, so the notifier must
// not close it: Close ends every subscription the process holds.
func TestNotifierCloseLeavesTheKubeconfigServiceOpen(t *testing.T) {
	d := newTestDeps(t)
	n := newKubeconfigNotifier(d.kubeconfigSvc)

	stop, err := n.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
	require.NoError(t, n.Close())

	sub := d.kubeconfigSvc.Subscribe()
	defer sub.Close()
	testutil.Recv(t, sub.Chan(), "the current config from a service still open")
}

// --- the seam ---

// fakeIdentitySource is a hub the test publishes into, standing in for kubeidentity's
// own workers: same per-context contract, driven by hand.
type fakeIdentitySource struct {
	hub *conflate.Hub[string, struct{}]
}

func newFakeIdentitySource() *fakeIdentitySource {
	return &fakeIdentitySource{
		hub: conflate.New[string, struct{}](func(_, next struct{}) (struct{}, bool) { return next, true }),
	}
}

func (f *fakeIdentitySource) Subscribe() kubeidentity.Subscription { return f.hub.Receiver() }

// publish reports that what is known about one context's server moved.
func (f *fakeIdentitySource) publish(contextName string) {
	_ = f.hub.Sender().Send(contextName, struct{}{})
}

// stringFeed is a hand-driven feed carrying a type no source here produces, so the loop
// is exercised against the seam rather than against its one implementation.
type stringFeed struct {
	ch     chan string
	closed *testutil.Signal
}

func (f stringFeed) Chan() <-chan string { return f.ch }
func (f stringFeed) Close()              { f.closed.Fire() }

// A notifier forwards the name its mapper builds, over whatever feed it was given.
func TestNotifierForwardsTheNameItsMapperBuilds(t *testing.T) {
	ch := make(chan string, 1)
	closed := testutil.NewSignal()

	n := newNotifier(
		func() feed[string] { return stringFeed{ch: ch, closed: closed} },
		func(v string) string { return "mapped/" + v },
	)
	startNotifier(t, n)

	ch <- "changed"
	assert.Equal(t, "mapped/changed", testutil.Recv(t, n.Names(), "the poke"))
}

// The loop owns the subscription, so stopping it is what releases the feed.
func TestNotifierReleasesItsFeedOnExit(t *testing.T) {
	closed := testutil.NewSignal()
	n := newNotifier(
		func() feed[string] { return stringFeed{ch: make(chan string), closed: closed} },
		func(v string) string { return v },
	)
	stop, err := n.Start(context.Background())
	require.NoError(t, err)

	require.NoError(t, stop(context.Background()))

	testutil.Wait(t, closed.Chan(), "the feed released on exit")
}
