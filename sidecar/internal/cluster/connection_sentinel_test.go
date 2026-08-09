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

// White-box (package cluster): the connection-sentinel tests drive the controller
// through SetSentinelWatcher, sharing the probe/spec helpers in core_controller_test.go.
package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"

	"github.com/amorey/beehive"
	"github.com/amorey/gochan/oneshot"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// fakeWatch is a controllable watch.Interface: its result channel stays open until Stop
// closes it. Closing the channel simulates the cluster's liveness watch breaking. The
// oneshot pair makes Stop idempotent by contract, so the sentinel's own deferred Stop is
// harmless.
type fakeWatch struct {
	tx *oneshot.Sender[watch.Event]
	rx *oneshot.Receiver[watch.Event]
}

func newFakeWatch() *fakeWatch {
	tx, rx := oneshot.New[watch.Event]()
	return &fakeWatch{tx: tx, rx: rx}
}

func (w *fakeWatch) Stop()                          { w.tx.Close() }
func (w *fakeWatch) ResultChan() <-chan watch.Event { return w.rx.Chan() }

// liveSentinelWatch is a SentinelWatchFunc that always returns a fresh, never-closing
// watch, so converge's sentinel stays open and never re-probes. Tests exercising the
// controller's other out-of-band triggers inject it so the real network-dialing
// sentinel watch never runs.
func liveSentinelWatch(context.Context, *rest.Config) (watch.Interface, error) {
	return newFakeWatch(), nil
}

// awaitWatch blocks until a sentinel watch is established, or fails on timeout.
func awaitWatch(t *testing.T, ch <-chan *fakeWatch) *fakeWatch {
	t.Helper()
	return testutil.Recv(t, ch, "a sentinel watch")
}

// TestClusterCoreControllerSentinelReprobesOnWatchBreak verifies the connection
// sentinel: after a successful probe the controller holds a liveness watch open, and
// when that watch closes it forces an out-of-band re-probe — fast detection owned by
// the connection controller, no sync engine involved.
func TestClusterCoreControllerSentinelReprobesOnWatchBreak(t *testing.T) {
	ctx := context.Background()

	probe, probeCh := signalingProbe()

	bh := NewTestBeehiveUnstarted(t)
	coreClient := beehive.NewClient[ClusterSpec, ClusterStatus](bh, ClusterGroupKind)
	w := NewStaticWatcher(t, testKubeConfig("alpha"))

	ctrl := NewClusterCoreController(&controllerRuntime{bh: bh}, w, probe, staticCheck(HealthPhaseHealthy))

	// Hand each established sentinel watch back to the test so it can break one.
	watches := make(chan *fakeWatch, 8)
	ctrl.SetSentinelWatcher(func(context.Context, *rest.Config) (watch.Interface, error) {
		fw := newFakeWatch()
		select {
		case watches <- fw:
		default:
		}
		return fw, nil
	})

	cc, err := beehive.Register(bh, ClusterGroupKind, ctrl)
	require.NoError(t, err)
	ctrl.SetControllerClient(cc)
	_, err = beehive.Register(bh, ClusterCacheGroupKind, &NoopController[ClusterCacheSpec, ClusterCacheStatus]{})
	require.NoError(t, err)
	stop, err := bh.Start(ctx)
	require.NoError(t, err)
	ctrl.StartBackground()
	t.Cleanup(func() { ctrl.StopBackground(); _ = stop(ctx) })

	_, err = coreClient.Create(ctx, kubeconfigName("alpha"), eligibleSpec("alpha"))
	require.NoError(t, err)

	// The initial scheduled reconcile probes once and, on success, opens a sentinel.
	awaitProbe(t, probeCh)
	drainProbes(probeCh)

	// Break the established liveness watch (simulating the connection dropping).
	fw := awaitWatch(t, watches)
	fw.Stop()

	// The break must force an out-of-band re-probe without waiting for the 30s poll.
	awaitProbe(t, probeCh)
}
