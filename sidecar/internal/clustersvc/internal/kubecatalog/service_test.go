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
	"fmt"
	"sync"
	"testing"

	"github.com/amorey/gobus/conflate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
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
	// serverUID is who every lease says it reached; empty reads as never identified,
	// which the sweep refuses the same way it refuses another cluster's.
	serverUID string
}

// testUID is the server every fake lease answers as, and the one the tests arm their
// subjects for. A sweep runs only where the two agree.
const testUID = "uid-1"

func newFakeConns() *fakeConns {
	return &fakeConns{hub: conflate.New[string, struct{}](), serverUID: testUID}
}

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

// ConnFor answers the way the pool does: the identity comes off the connection, so an
// unidentified one is refused rather than paired with whatever the probes last said.
func (l *fakeLease) ConnFor(ctx context.Context, serverUID string) (*kubeconn.Connection, error) {
	conn, err := l.Conn(ctx)
	if err != nil {
		return nil, err
	}

	l.conns.mu.Lock()
	defer l.conns.mu.Unlock()
	switch {
	case l.conns.serverUID == "":
		return nil, fmt.Errorf("%w: unidentified", kubeconn.ErrIdentityMismatch)
	case l.conns.serverUID != serverUID:
		return nil, fmt.Errorf("%w: reached %s", kubeconn.ErrIdentityMismatch, l.conns.serverUID)
	}
	return conn, nil
}

// State carries no identity on purpose: this package reads it off the connection, and a
// fake that also served it here would let the correlation back in unnoticed.
func (l *fakeLease) State() kubeconn.State { return kubeconn.State{} }

func (f *fakeConns) setServerUID(uid string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.serverUID = uid
}

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

	svc.Track("cachedcatalog/1", "prod", testUID)
	svc.Track("cachedcatalog/1", "prod", testUID)

	assert.Equal(t, []string{"prod"}, conns.acquired)
	_, ok := svc.Read("cachedcatalog/1")
	assert.True(t, ok)
}

// Forget gives back everything Track took: the claim, the subject, and the published
// baseline behind the signal.
func TestForgetReleasesWhatTrackTook(t *testing.T) {
	conns := newFakeConns()
	svc := newWithOptions(conns, answering(pods))
	svc.Track("cachedcatalog/1", "prod", testUID)

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
	svc.Track("cachedcatalog/1", "prod", testUID)
	svc.Track("cachedcatalog/2", "staging", testUID)

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

	svc.Track("cachedcatalog/1", "prod", testUID)

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

	svc.Track("cachedcatalog/1", "prod", testUID)
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

// --- identity scoping ---

// A context re-pointed at another cluster answers as a server this subject is not for,
// and the pool wakes every subject over that context the moment its identity moves — so
// the superseded cache's sweep is the first thing to run against the new server. Reading
// that server's kinds into this cache is what the check exists to stop.
func TestSweepRefusesAnotherClustersServer(t *testing.T) {
	conns := newFakeConns()
	conns.setServerUID("uid-2")
	svc := newWithOptions(conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)

	testutil.Recv(t, sub.Chan(), "the refusal's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.False(t, obs.Known(), "nothing was asked, so nothing was committed")
	assert.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)
	assert.Equal(t, probe.VerdictSuspended, obs.LastAttempt.Verdict)
}

// The standing answer survives the refusal: a cache that swept, then had its context
// re-pointed, keeps the kinds it read from its own server until the record is disarmed.
func TestSweepKeepsItsAnswerWhenTheServerChanges(t *testing.T) {
	conns := newFakeConns()
	svc := newWithOptions(conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the first answer")

	conns.setServerUID("uid-2")
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the refusal's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds, "read from this cache's own server")
	assert.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)
}

// A server that has not said which cluster it is cannot confirm the subject either, so
// the sweep parks — and the pool reporting the identity is what re-arms it. The gap
// between a connection answering and the UID probe behind it answering.
func TestSweepWaitsForAnUnidentifiedServer(t *testing.T) {
	conns := newFakeConns()
	conns.setServerUID("")
	svc := newWithOptions(conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the wait's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	require.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)

	conns.setServerUID(testUID)
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the sweep once the server is identified")
	obs, ok = svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds)
}

// A cluster nothing reached reports the outage, not the identity it could not have read
// either way — so a cold start reads as connecting rather than as identity trouble.
func TestSweepReportsTheOutageBeforeTheIdentity(t *testing.T) {
	conns := newFakeConns()
	conns.setConnErr(kubeconn.ErrNoConnection)
	conns.setServerUID("")
	svc := newWithOptions(conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)

	testutil.Recv(t, sub.Chan(), "the suspension's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, ReasonNoConnection, obs.LastAttempt.Reason)
}

// The defect the connection-carried identity exists for: a context re-pointed at another
// cluster commits its new connection before the UID probe re-runs over it, so for a dispatch
// plus a round-trip the pool holds a fresh connection beside the previous one's UID. Asking
// the connection who it reached refuses that; correlating it with the last observation
// accepted it and swept the wrong cluster.
func TestSweepRefusesAConnectionNotYetReidentified(t *testing.T) {
	conns := newFakeConns()
	svc := newWithOptions(conns, answering(pods))
	startService(t, svc)
	sub := svc.Subscribe()
	t.Cleanup(sub.Close)

	svc.Track("cachedcatalog/1", "prod", testUID)
	testutil.Recv(t, sub.Chan(), "the first answer")

	// The connection is replaced and nothing has identified the new one yet.
	conns.setServerUID("")
	conns.publish("prod")

	testutil.Recv(t, sub.Chan(), "the refusal's signal")
	obs, ok := svc.Read("cachedcatalog/1")
	require.True(t, ok)
	assert.Equal(t, ReasonIdentityMismatch, obs.LastAttempt.Reason)
	assert.Equal(t, []Kind{pods}, obs.Value.Kinds, "the answer from this cache's own server stands")
}
