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

package kubeidentity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// testBudget paces a loop off the wall clock so a test never outwaits production's
// numbers.
var testBudget = Budget{Interval: time.Millisecond}

// The shape the cluster service composes this into.
var _ lifecycle.StartCloser = (*Service)(nil)

// An unprobed context reports nothing known rather than an empty identity, which is the
// distinction a caller renders: "connecting" is not "connected to a server with no UID".
// Registering it is not answering it, so the Get that registers reads this way too.
func TestGetReportsNothingKnown(t *testing.T) {
	s := New(testBudget)
	defer s.Close()

	state, known := s.Get("prod")

	assert.False(t, known)
	assert.Equal(t, State{}, state)
}

// The read half of what a loop writes.
func TestGetReportsEntry(t *testing.T) {
	s := New(testBudget)
	defer s.Close()
	s.entries["prod"] = entry{state: State{Err: ErrProbe}, known: true}

	state, known := s.Get("prod")

	assert.True(t, known)
	assert.ErrorIs(t, state.Err, ErrProbe)
}

// Get is the read a reconcile pass makes, so it must answer before anything is started:
// a pass reaching a service still starting would otherwise block or panic where it is
// documented to do neither. The context it registers gets its loop all the same.
func TestGetAnswersBeforeStart(t *testing.T) {
	s := New(testBudget)

	_, known := s.Get("prod")
	assert.False(t, known)

	stop, err := s.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
	assert.NoError(t, s.Close())
}

// lifecycle calls a stop func more than once on an unwind, so a second call must be a
// no-op rather than a panic or a hang.
func TestStopIsIdempotent(t *testing.T) {
	s := New(testBudget)
	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	s.Get("prod")

	require.NoError(t, stop(context.Background()))
	require.NoError(t, stop(context.Background()))
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

// The trigger subscribes at startup and parks on this, so it has to hand back a usable
// receiver long before anything sends.
func TestSubscribeIsUsableBeforeAnythingSends(t *testing.T) {
	s := New(testBudget)
	defer s.Close()

	sub := s.Subscribe()
	defer sub.Close()

	require.NotNil(t, sub)
	assert.NotNil(t, sub.Chan())
}

// A Get is what starts a loop, and the stop func is what ends it: one left running past
// the drain would outlive the service that owns it.
func TestGetStartsALoopTheDrainEnds(t *testing.T) {
	s := New(testBudget)
	stop, err := s.Start(t.Context())
	require.NoError(t, err)

	s.Get("prod")

	testutil.WaitReturn(t, func() {
		require.NoError(t, stop(context.Background()))
	}, "drain of the loop Get started")
	require.NoError(t, s.Close())
}

// Registering twice is one loop, not two: an entry and its loop are the same fact, and a
// second would pace the same context against a cadence nothing tracks.
func TestGetRegistersOnce(t *testing.T) {
	s := New(testBudget)
	defer s.Close()

	s.Get("prod")
	s.Get("prod")

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Len(t, s.entries, 1)
}

// Forget is the other half of registration: it takes the row and the loop reading it
// together, since one exists exactly while the other does.
func TestForgetEndsTheLoop(t *testing.T) {
	s := New(testBudget)
	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	s.Get("prod")
	s.Forget("prod")

	// The drain is what waits for it, and it would hang on a loop Forget left running.
	testutil.WaitReturn(t, func() {
		require.NoError(t, stop(context.Background()))
	}, "drain of a forgotten context's loop")

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Empty(t, s.entries)
}

// Forgetting is a caller saying it will not ask again, which it may say twice, or about a
// context it never asked about.
func TestForgetIsIdempotent(t *testing.T) {
	s := New(testBudget)
	defer s.Close()

	s.Forget("never-registered")
	s.Get("prod")
	s.Forget("prod")
	s.Forget("prod")

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Empty(t, s.entries)
}

// Asking again after forgetting starts over: the caller changed its mind, and a fresh
// registration owes it a fresh loop rather than one already unwinding.
func TestGetAfterForgetRegistersAgain(t *testing.T) {
	s := New(testBudget)
	defer s.Close()

	s.Get("prod")
	s.store("prod", State{Identity: Identity{ServerUID: "uid-1"}})
	s.Forget("prod")

	state, known := s.Get("prod")
	assert.False(t, known, "a forgotten context is forgotten, not remembered as stale")
	assert.Equal(t, State{}, state)
}

// A probe that lands after its context was forgotten has nowhere to report. Storing it
// would put the row back without the loop deleted with it — one nothing probes, and
// nothing forgets again.
func TestStoreIgnoresAForgottenContext(t *testing.T) {
	s := New(testBudget)
	sub := s.Subscribe()
	defer sub.Close()

	s.Get("prod")
	s.Forget("prod")
	s.store("prod", State{Identity: Identity{ServerUID: "uid-1"}})

	_, known := s.Get("prod")
	assert.False(t, known)
	testutil.NoRecv(t, sub.Chan(), 50*time.Millisecond, "news about a forgotten context")
}

// A moved answer wakes the caller; one that repeats must not. Every signal costs the
// caller a pass, and its dependents a round of work — per context, per interval, forever.
func TestStorePublishesOnlyWhenTheAnswerMoved(t *testing.T) {
	s := New(testBudget)
	defer s.Close()
	sub := s.Subscribe()
	defer sub.Close()

	// A store only lands on a registered context, which is what a probe runs for.
	s.Get("prod")

	state := State{Identity: Identity{ServerUID: "uid-1"}}

	s.store("prod", state)
	assert.Equal(t, "prod", testutil.Recv(t, sub.Chan(), "first answer").Key)

	s.store("prod", state)
	testutil.NoRecv(t, sub.Chan(), 50*time.Millisecond, "repeat of the same answer")

	s.store("prod", State{Err: errors.New("boom")})
	assert.Equal(t, "prod", testutil.Recv(t, sub.Chan(), "changed answer").Key)
}

// A failure that repeats is not news either. The identities match, so the reasons are
// what decide it — and a caller woken to re-read the same failure is the same cost as
// one woken to re-read the same success.
func TestStorePublishesOnlyWhenTheFailureMoved(t *testing.T) {
	s := New(testBudget)
	defer s.Close()
	sub := s.Subscribe()
	defer sub.Close()

	// A store only lands on a registered context, which is what a probe runs for.
	s.Get("prod")

	s.store("prod", State{Err: errors.New("connection refused")})
	assert.Equal(t, "prod", testutil.Recv(t, sub.Chan(), "first failure").Key)

	s.store("prod", State{Err: errors.New("connection refused")})
	testutil.NoRecv(t, sub.Chan(), 50*time.Millisecond, "repeat of the same failure")

	s.store("prod", State{Err: errors.New("401 Unauthorized")})
	assert.Equal(t, "prod", testutil.Recv(t, sub.Chan(), "changed failure").Key)
}

// Answers arriving faster than the reader drains them coalesce into one wake. That is
// what the empty value buys: the key is the news, so a reader that missed a step still
// reads what Get says now rather than replaying what it said then.
func TestSignalsForOneContextCoalesce(t *testing.T) {
	s := New(testBudget)
	defer s.Close()
	sub := s.Subscribe()
	defer sub.Close()

	// A store only lands on a registered context, which is what a probe runs for.
	s.Get("prod")

	s.store("prod", State{Identity: Identity{ServerUID: "uid-1"}})
	s.store("prod", State{Identity: Identity{ServerUID: "uid-2"}})

	assert.Equal(t, "prod", testutil.Recv(t, sub.Chan(), "coalesced wake").Key)
	testutil.NoRecv(t, sub.Chan(), 50*time.Millisecond, "a second wake for the same context")

	state, _ := s.Get("prod")
	assert.Equal(t, "uid-2", state.Identity.ServerUID)
}

// The first answer is news even when it is the zero state: a caller reads "nothing known"
// until something is stored, so the move out of it is the whole point.
func TestStorePublishesTheFirstAnswer(t *testing.T) {
	s := New(testBudget)
	defer s.Close()
	sub := s.Subscribe()
	defer sub.Close()

	// A store only lands on a registered context, which is what a probe runs for.
	s.Get("prod")

	s.store("prod", State{})

	assert.Equal(t, "prod", testutil.Recv(t, sub.Chan(), "first answer").Key)
	state, known := s.Get("prod")
	assert.True(t, known)
	assert.Equal(t, State{}, state)
}

// Registration must be refused once the drain has begun: a loop started after it would
// join a WaitGroup already being waited on.
func TestGetDoesNotRegisterDuringShutdown(t *testing.T) {
	s := New(testBudget)
	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))

	_, known := s.Get("prod")
	assert.False(t, known)

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Empty(t, s.entries)
}

// Close latches the same door. It cancels the loop context, so a Get past it would
// otherwise register an entry whose loop ends the moment it starts — leaving a row
// nothing probes.
func TestGetDoesNotRegisterAfterClose(t *testing.T) {
	s := New(testBudget)
	require.NoError(t, s.Close())

	_, known := s.Get("prod")
	assert.False(t, known)

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Empty(t, s.entries)
}
