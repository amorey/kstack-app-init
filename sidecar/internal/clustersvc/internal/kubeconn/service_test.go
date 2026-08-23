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

package kubeconn

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

var runAt = time.Date(2026, 8, 23, 10, 5, 0, 0, time.UTC)

// fakeKubeconfig resolves each context to a key the test controls, standing in for the user's
// file. err, when set, is what every context fails with.
type fakeKubeconfig struct {
	mu   sync.Mutex
	keys map[string]string
	err  error
	hub  *watch.Hub[*api.Config]
}

// resolving is a kubeconfig where each context resolves to the key beside it.
func resolving(contextKeys ...string) *fakeKubeconfig {
	f := &fakeKubeconfig{keys: map[string]string{}}
	for i := 0; i < len(contextKeys); i += 2 {
		f.keys[contextKeys[i]] = contextKeys[i+1]
	}
	return f
}

// rotate is the user editing their file: contextName now names different credentials. An
// empty key removes the context.
func (f *fakeKubeconfig) rotate(contextName, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if key == "" {
		delete(f.keys, contextName)
		return
	}
	f.keys[contextName] = key
}

func (f *fakeKubeconfig) RESTConfig(contextName string) (*rest.Config, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, "", f.err
	}
	key, ok := f.keys[contextName]
	if !ok {
		return nil, "", kubeconfig.ErrContextNotFound
	}
	return &rest.Config{Host: "https://" + key}, key, nil
}

func (f *fakeKubeconfig) Subscribe() kubeconfig.Subscription {
	if f.hub == nil {
		f.hub = watch.New[*api.Config](nil)
	}
	return f.hub.Receiver()
}

// A check the prober records without dispatching has no duration: subtracting a zero
// StartedAt would report the time since the zero year.
func TestLatencyIsZeroForARunThatNeverStarted(t *testing.T) {
	a := Attempt{ScheduledAt: runAt, FinishedAt: runAt, Reason: ReasonDependencyFailed}

	assert.True(t, a.Done())
	assert.False(t, a.Running())
	assert.Zero(t, a.Latency())
}

func TestLatencyMeasuresADispatchedRun(t *testing.T) {
	a := Attempt{ScheduledAt: runAt, StartedAt: runAt, FinishedAt: runAt.Add(2 * time.Second)}

	assert.Equal(t, 2*time.Second, a.Latency())
}

// A suspended check keeps its last answer and schedules nothing.
func TestSuspendedCheckKeepsItsAnswer(t *testing.T) {
	o := Observation[string]{
		Value:       "abc-123",
		LastSeen:    runAt,
		LastAttempt: Attempt{ScheduledAt: runAt, FinishedAt: runAt, Reason: ReasonDependencyFailed},
	}

	assert.True(t, o.Known(), "the UID it read is still the UID it read")
	assert.False(t, o.OK())
	assert.False(t, o.Scheduled(), "nothing is due until the connection comes back")
	assert.False(t, o.InFlight())
}

// Identity projects the three scalars a connection is scoped to out of the observations
// carrying them, so retiring one stays a ==.
func TestIdentityProjectsTheCheckedScalars(t *testing.T) {
	s := State{
		ServerUID:     Observation[string]{Value: "uid-1", LastSeen: runAt},
		ServerVersion: Observation[VersionInfo]{Value: VersionInfo{GitVersion: "v1.29.3"}, LastSeen: runAt},
		Principal: Observation[Principal]{
			Value:    Principal{Username: "admin@example", Groups: []string{"system:masters"}},
			LastSeen: runAt,
		},
	}

	assert.Equal(t, Identity{
		ServerUID:     "uid-1",
		ServerVersion: "v1.29.3",
		Username:      "admin@example",
	}, s.Identity())
}

// A part no probe could read is empty rather than absent, which is what lets two
// connections missing the same part compare equal.
func TestIdentityLeavesAnUnreadPartEmpty(t *testing.T) {
	forbidden := State{
		ServerUID: Observation[string]{LastAttempt: Attempt{FinishedAt: runAt, Reason: ReasonForbidden}},
		Principal: Observation[Principal]{Value: Principal{Username: "reader@example"}, LastSeen: runAt},
	}

	assert.Equal(t, Identity{Username: "reader@example"}, forbidden.Identity())
	assert.Equal(t, forbidden.Identity(), State{
		ServerUID: Observation[string]{LastAttempt: Attempt{FinishedAt: runAt, Reason: ReasonUnsupported}},
		Principal: Observation[Principal]{Value: Principal{Username: "reader@example"}, LastSeen: runAt},
	}.Identity(), "why the UID is missing is the observation's, not the identity's")
}

// The pre-read kubeconfig is empty, so every context looks departed. Refusing the claim
// would report a live cluster's credentials broken for as long as the first read takes.
func TestAcquireClaimsAContextTheKubeconfigHasNotReadYet(t *testing.T) {
	s := New(&fakeKubeconfig{err: kubeconfig.ErrNotRead})

	lease, err := s.Acquire("prod")

	require.NoError(t, err)
	assert.NotNil(t, lease)
}

// A context the file genuinely does not resolve is refused with the reader's own error,
// rather than becoming a claim on credentials nobody could build.
func TestAcquireRefusesAContextThatWillNotResolve(t *testing.T) {
	boom := errors.New("no such certificate authority file")
	s := New(&fakeKubeconfig{err: boom})

	_, err := s.Acquire("prod")

	assert.ErrorIs(t, err, boom)
}

// The subscription is established before Start returns, so a config sent straight after it
// is read rather than dropped, and stop joins the loop instead of leaving it running.
func TestStartWatchesTheKubeconfigUntilStopped(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)

	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cfg.hub, "Start subscribes before it returns")
	cfg.hub.Sender().Send(&api.Config{})

	require.NoError(t, stop(t.Context()))
}

// --- claims and connections ---

// Contexts that resolve alike are deliberately not merged: each gets its own connection, so
// what a probe reports belongs to one context and nothing has to be apportioned.
func TestAcquireBuildsAConnectionPerContext(t *testing.T) {
	s := New(resolving("prod", "key-1", "prod-admin", "key-1"))

	defer must(s.Acquire("prod")).Release()
	defer must(s.Acquire("prod-admin")).Release()

	assert.Len(t, s.claimed, 2)
	assert.NotSame(t, s.claimed["prod"], s.claimed["prod-admin"])
}

// An entry nobody holds is one nothing probes, so the last release has to drop it.
func TestReleaseDropsTheEntryTheLastHolderHeld(t *testing.T) {
	s := New(resolving("prod", "key-1"))

	must(s.Acquire("prod")).Release()

	assert.Empty(t, s.claimed)
}

// Two holders of one context — the cluster pass and a log tail — are two claims. The first to
// finish must not stop the other's probe.
func TestReleaseKeepsTheEntryWhileAnotherHolderHoldsIt(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	pass := must(s.Acquire("prod"))
	tail := must(s.Acquire("prod"))

	pass.Release()
	assert.Len(t, s.claimed, 1)

	tail.Release()
	assert.Empty(t, s.claimed)
}

// Release is idempotent by contract, so it is safe to defer beside a path that also releases.
func TestReleaseIsIdempotent(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := must(s.Acquire("prod"))
	other := must(s.Acquire("prod"))

	lease.Release()
	lease.Release()

	assert.Len(t, s.claimed, 1, "the second release must not drop the other holder's entry")
	other.Release()
}

// The whole point of claiming a context rather than a fingerprint: credentials rotate under a
// context that never moves, and the claim has to follow them.
func TestRekeyRebuildsOnRotatedCredentials(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	defer must(s.Acquire("prod")).Release()
	require.Equal(t, "key-1", s.claimed["prod"].fingerprint)

	cfg.rotate("prod", "key-2")
	s.rekey()

	assert.Equal(t, "key-2", s.claimed["prod"].fingerprint)
}

// A rotation is a different connection, so what the old one answered does not carry over: the
// server behind the new credentials has yet to say anything.
func TestRekeyForgetsWhatTheOldCredentialsRead(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := must(s.Acquire("prod"))
	defer lease.Release()
	s.claimed["prod"].state = State{ServerUID: Observation[string]{Value: "uid-1", LastSeen: runAt}}

	cfg.rotate("prod", "key-2")
	s.rekey()

	assert.Equal(t, State{}, lease.State())
}

// Unchanged credentials keep the connection they built, or every kubeconfig write would drop
// every cluster's probe and start its ladder again.
func TestRekeyKeepsAConnectionItsCredentialsStillName(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := must(s.Acquire("prod"))
	defer lease.Release()
	probed := State{ServerUID: Observation[string]{Value: "uid-1", LastSeen: runAt}}
	s.claimed["prod"].state = probed

	s.rekey()

	assert.Equal(t, probed, lease.State())
}

// A claim taken before the kubeconfig's first read holds a context that resolves to nothing.
// The read is what builds it, which is why the pool subscribes rather than waiting to be asked.
func TestRekeyBuildsAClaimTakenBeforeTheKubeconfigWasRead(t *testing.T) {
	cfg := &fakeKubeconfig{err: kubeconfig.ErrNotRead}
	s := New(cfg)
	defer must(s.Acquire("prod")).Release()
	require.Empty(t, s.claimed["prod"].fingerprint, "nothing resolves yet")

	cfg.err = nil
	cfg.keys = map[string]string{"prod": "key-1"}
	s.rekey()

	assert.Equal(t, "key-1", s.claimed["prod"].fingerprint)
}

// A context the user deleted stops resolving. Dropping the connection is what stops the claim
// reporting the server it used to reach; keeping it would probe credentials nobody names.
func TestRekeyDropsAContextThatStoppedResolving(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := must(s.Acquire("prod"))
	defer lease.Release()

	cfg.rotate("prod", "")
	s.rekey()

	assert.Empty(t, s.claimed["prod"].fingerprint, "the claim is still held, the connection is not")
	assert.Equal(t, State{}, lease.State(), "a claim on nothing knows nothing")
}

// must fails the test rather than returning an error a caller would have to check, since
// every Acquire here is over a kubeconfig the test wrote.
func must(lease Lease, err error) Lease {
	if err != nil {
		panic(err)
	}
	return lease
}

// A receiver is bound to its key for life, so keying the state hub on the entry would strand
// a watcher the moment its claim re-keyed. Keyed by context, one subscription follows the
// claim across the move. The send stands in for the probe that will make it.
func TestWatchStateFollowsTheClaimAcrossARekey(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	lease := must(s.Acquire("prod"))
	defer lease.Release()
	sub := lease.WatchState()
	defer sub.Close()

	cfg.rotate("prod", "key-2")
	s.rekey()
	s.stateHub.Sender().Send("prod", State{ServerUID: Observation[string]{Value: "uid-1", LastSeen: runAt}})

	ev, err := sub.RecvContext(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "uid-1", ev.Value.ServerUID.Value)
}

// rekey resolves from a snapshot taken without the lock, so the context it comes back to build
// may have been released meanwhile. Building anyway would resurrect the entry that release just
// dropped, and nothing would ever claim it again.
func TestBuildIgnoresAContextNobodyClaims(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	must(s.Acquire("prod")).Release()
	require.Empty(t, s.claimed)

	s.mu.Lock()
	s.buildLocked("prod", "key-1")
	s.mu.Unlock()

	assert.Empty(t, s.claimed)
}
