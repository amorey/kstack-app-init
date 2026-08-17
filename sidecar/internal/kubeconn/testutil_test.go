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

// Fixtures shared across this package's test files.
package kubeconn

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// testCadence paces probing in tests, shrunk from DefaultBudget through the constructor
// so nothing here encodes production's number.
const testCadence = 10 * time.Millisecond

// testConcurrency is the slot count these tests reason about, small enough that a
// bounded-fan-out test stays quick.
const testConcurrency = 4

var testBudget = Budget{
	Cadence:     testCadence,
	RetryBase:   testCadence,
	RetryMax:    2 * testCadence,
	Timeout:     testCadence,
	Concurrency: testConcurrency,
}

// newTestService is a pool with no probe of its own — for the tests that only care about
// pooling, which must never reach the network.
func newTestService() *Service {
	svc := New(testBudget)
	svc.probe = func(context.Context, *Connection) (Identity, error) { return Identity{}, nil }
	return svc
}

// fixture is a started Service plus the seams a probe test drives it through.
type fixture struct {
	*Service
	probes *testutil.Probe[struct{}] // one value per probe call
}

// newProbeFixture returns a started service whose probe returns what probe says.
func newProbeFixture(t *testing.T, probe func(context.Context, *Connection) (Identity, error)) *fixture {
	t.Helper()

	f := &fixture{
		Service: New(testBudget),
		probes:  testutil.NewProbe[struct{}](8),
	}
	f.Service.probe = func(ctx context.Context, conn *Connection) (Identity, error) {
		f.probes.Fire(struct{}{})
		return probe(ctx, conn)
	}

	stop, err := f.Service.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, f.Service.Close())
	})
	return f
}

// watchNews drains one key's announcements into a probe, so a test asserts on them the same
// way it does on probes. The subscription is closed on cleanup, which is what ends the
// reader — before the service's own teardown, since cleanups run last-registered first.
func (f *fixture) watchNews(t *testing.T, key string) *testutil.Probe[string] {
	t.Helper()

	sub := f.Subscribe(key)
	fired := testutil.NewProbe[string](8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			ev, err := sub.Recv()
			if err != nil {
				return
			}
			fired.Fire(ev.Key)
		}
	}()
	t.Cleanup(func() {
		sub.Close()
		testutil.Wait(t, done, "the news reader to end")
	})
	return fired
}

// probesTo is the ordinary case: every probe answers the same way.
func probesTo(id Identity) func(context.Context, *Connection) (Identity, error) {
	return func(context.Context, *Connection) (Identity, error) { return id, nil }
}

// testConfig is credentials nobody dials — the pool builds a transport from them and the
// probe seam stands in for what would go over it.
func testConfig() *rest.Config { return &rest.Config{Host: "https://one.example"} }

// acquire claims a key and releases it when the test ends.
func (f *fixture) acquire(t *testing.T, key string) Lease {
	t.Helper()
	lease, err := f.Acquire(testConfig(), key)
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	return lease
}

// running reports whether a loop goroutine exists for a key.
func (f *fixture) running(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.conns[key]
	return ok && e.running
}

// asked reports whether a key still has a probe request queued. It goes false once the
// loop has taken the kick, which is how a test knows the loop is past its own select and
// into the probe.
func (f *fixture) asked(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.conns[key]
	return ok && pending(e)
}

// awaitResult waits for a key's first result to land.
func (f *fixture) awaitResult(t *testing.T, key string) *Result {
	t.Helper()
	require.Eventually(t, func() bool { return f.State(key).Last != nil }, testutil.Timeout, testCadence)
	return f.State(key).Last
}
