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

package kubecatalog

import (
	"context"
	"sync"
	"testing"

	"github.com/amorey/gobus/conflate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// fakeConns stands in for the pool: every lease's Conn answers connErr or an empty
// connection, and the hub is the fleet feed the bridge reads, driven by hand.
type fakeConns struct {
	hub *conflate.Hub[string, struct{}]

	mu       sync.Mutex
	acquired []string
	released int
	connErr  error
}

func newFakeConns() *fakeConns { return &fakeConns{hub: conflate.New[string, struct{}]()} }

func (f *fakeConns) Acquire(contextName string) kubeconn.Lease {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquired = append(f.acquired, contextName)
	return &fakeLease{conns: f}
}

func (f *fakeConns) Subscribe() kubeconn.Subscription { return f.hub.Receiver() }

// publish is a connection's news landing on the fleet feed.
func (f *fakeConns) publish(contextName string) { f.hub.Sender().Send(contextName, struct{}{}) }

func (f *fakeConns) setConnErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connErr = err
}

func (f *fakeConns) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.released
}

type fakeLease struct{ conns *fakeConns }

func (l *fakeLease) Conn(context.Context) (*kubeconn.Connection, error) {
	l.conns.mu.Lock()
	defer l.conns.mu.Unlock()
	if l.conns.connErr != nil {
		return nil, l.conns.connErr
	}
	return &kubeconn.Connection{}, nil
}

func (l *fakeLease) State() kubeconn.State { return kubeconn.State{} }

// WatchState is unused by this package; nothing here watches a single claim.
func (l *fakeLease) WatchState() kubeconn.StateSubscription { return nil }

func (l *fakeLease) Departed() bool { return false }

func (l *fakeLease) Release() {
	l.conns.mu.Lock()
	defer l.conns.mu.Unlock()
	l.conns.released++
}

// answering is a sweep that always serves these kinds.
func answering(kinds ...Kind) option {
	return withSweep(func(*kubeconn.Connection) (Catalog, error) {
		return Catalog{Kinds: kinds}, nil
	})
}

// startService runs the service for the test's life, stop before Close like the
// composition root.
func startService(t *testing.T, s *Service) {
	t.Helper()
	stop, err := s.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, s.Close())
	})
}

// One claim per id, however many passes arm it: the context is fixed for the id's life,
// so a repeat is a no-op.
func TestTrackIsIdempotent(t *testing.T) {
	conns := newFakeConns()
	svc := newWithOptions(conns, answering(pods))

	svc.Track("cachedcatalog/1", "prod")
	svc.Track("cachedcatalog/1", "prod")

	assert.Equal(t, []string{"prod"}, conns.acquired)
	_, ok := svc.Read("cachedcatalog/1")
	assert.True(t, ok)
}

// Forget gives back everything Track took: the claim, the subject, and the published
// baseline behind the signal.
func TestForgetReleasesWhatTrackTook(t *testing.T) {
	conns := newFakeConns()
	svc := newWithOptions(conns, answering(pods))
	svc.Track("cachedcatalog/1", "prod")

	svc.Forget("cachedcatalog/1")

	assert.Equal(t, 1, conns.releaseCount())
	_, ok := svc.Read("cachedcatalog/1")
	assert.False(t, ok)

	svc.Forget("cachedcatalog/1")
	assert.Equal(t, 1, conns.releaseCount(), "idempotent")
}

// Close gives the pool every claim back: the pool closes after this service, and a
// claim outliving its holder is the holder's bug.
func TestCloseReleasesEveryLease(t *testing.T) {
	conns := newFakeConns()
	svc := newWithOptions(conns, answering(pods))
	svc.Track("cachedcatalog/1", "prod")
	svc.Track("cachedcatalog/2", "staging")

	require.NoError(t, svc.Close())

	assert.Equal(t, 2, conns.releaseCount())
}

// The whole read path: arming dispatches a sweep, the committed answer signals the
// subscriber, and Read hands it back beside its attempts.
func TestSweepSignalsWhenTheAnswerLands(t *testing.T) {
	conns := newFakeConns()
	svc := newWithOptions(conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod")

	ev := testutil.Recv(t, sub.Chan(), "the sweep's signal")
	assert.Equal(t, "cachedcatalog/1", ev.Key)
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.True(t, obs.Known())
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds)
	assert.True(t, obs.OK())
}

// A subject whose context resolves to nothing suspends, and the connection bridge is
// what brings it back: the pool saying the context moved re-runs the sweep with no
// cadence to wait out.
func TestConnectionRecoveryWakesASuspendedSweep(t *testing.T) {
	conns := newFakeConns()
	conns.setConnErr(kubeconn.ErrNoConnection)
	svc := newWithOptions(conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod")
	testutil.Recv(t, sub.Chan(), "the suspension's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.False(t, obs.Known())

	conns.setConnErr(nil)
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the recovery's signal")
	obs, ok = svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.True(t, obs.Known())
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds)
}
