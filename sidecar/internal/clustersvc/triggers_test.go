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

// startTrigger starts tr and joins it on cleanup.
func startTrigger[T any](t *testing.T, tr *trigger[T]) {
	t.Helper()
	stop, err := tr.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
}

// The subscription is current-on-subscribe, so whatever the file already held names the
// anchor at startup rather than waiting for the user to edit it.
func TestKubeconfigTriggerWakesTheAnchorForTheStartupSnapshot(t *testing.T) {
	tr := newKubeconfigTrigger(newFakeKubeconfigSource(cfgWith("prod")))
	startTrigger(t, tr)

	assert.Equal(t, ClusterSourceNameKubeconfig, testutil.Recv(t, tr.Wakes(), "the startup poke"))
}

// Every change names the same record: which contexts moved is the anchor's pass to work
// out, so the trigger translates rather than deciding.
func TestKubeconfigTriggerWakesTheAnchorForEachSnapshot(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	tr := newKubeconfigTrigger(src)
	startTrigger(t, tr)
	testutil.Recv(t, tr.Wakes(), "the startup poke")

	src.publish(cfgWith("prod", "staging"))

	assert.Equal(t, ClusterSourceNameKubeconfig, testutil.Recv(t, tr.Wakes(), "the second poke"))
}

// The context that answered is the record that moved, and mapping the two is the one
// thing beehive cannot do: only this package knows the record's name.
func TestKubeidentityTriggerWakesTheContextsCluster(t *testing.T) {
	src := newFakeIdentitySource()
	tr := newKubeidentityTrigger(src)
	startTrigger(t, tr)

	src.publish("staging")

	assert.Equal(t, KubeconfigName("staging"), testutil.Recv(t, tr.Wakes(), "the poke"))
}

// The watcher shutting down ends the loop, and closing the channel is what ends
// beehive's read of it — it drains what was already sent and stops.
func TestTriggerClosesItsChannelWhenTheSourceCloses(t *testing.T) {
	src := newFakeKubeconfigSource(cfgWith("prod"))
	tr := newKubeconfigTrigger(src)
	startTrigger(t, tr)
	testutil.Recv(t, tr.Wakes(), "the startup poke")

	src.close()

	testutil.WaitClosed(t, tr.Wakes(), "the wake channel to close with the feed")
}

// The stop func must join the loop even mid-send, so service.Close never races one.
// Nothing reads Wakes here, so the loop is parked on exactly that send.
func TestTriggerStopJoinsTheLoop(t *testing.T) {
	tr := newKubeconfigTrigger(newFakeKubeconfigSource(cfgWith("prod")))
	stop, err := tr.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")
}

// The app owns the kubeconfig service and hands it to every reader, so stopping the
// trigger must release only its own subscription: the service's Close ends every
// subscription the process holds.
func TestTriggerStopLeavesTheKubeconfigServiceOpen(t *testing.T) {
	d := newTestDeps(t)
	tr := newKubeconfigTrigger(d.kubeconfigSvc)

	stop, err := tr.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))

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
		hub: conflate.New[string](func(_, next struct{}) (struct{}, bool) { return next, true }),
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

// A trigger forwards the name its mapper builds, over whatever feed it was given.
func TestTriggerForwardsTheNameItsMapperBuilds(t *testing.T) {
	ch := make(chan string, 1)
	closed := testutil.NewSignal()

	tr := newTrigger(
		func() feed[string] { return stringFeed{ch: ch, closed: closed} },
		func(v string) string { return "mapped/" + v },
	)
	startTrigger(t, tr)

	ch <- "changed"
	assert.Equal(t, "mapped/changed", testutil.Recv(t, tr.Wakes(), "the poke"))
}

// The loop owns the subscription, so stopping it is what releases the feed.
func TestTriggerReleasesItsFeedOnExit(t *testing.T) {
	closed := testutil.NewSignal()
	tr := newTrigger(
		func() feed[string] { return stringFeed{ch: make(chan string), closed: closed} },
		func(v string) string { return v },
	)
	stop, err := tr.Start(context.Background())
	require.NoError(t, err)

	require.NoError(t, stop(context.Background()))

	testutil.Wait(t, closed.Chan(), "the feed released on exit")
}
