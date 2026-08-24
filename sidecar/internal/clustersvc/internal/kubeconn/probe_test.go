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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// connect runs the connection probe's body once, on the test's goroutine, and applies its
// commit the way the engine would.
func connect(t *testing.T, cfg *fakeKubeconfig, v values) (probe.Result, values) {
	t.Helper()
	res, commit := runConnection(cfg)(t.Context(), "prod", v)
	if commit != nil {
		commit(&v)
	}
	return res, v
}

// --- the connection probe's classifications ---

// A context that left the file has nothing to reach, so its probe suspends — the file is the
// whole truth about presence, and the watch reports it moving, so polling asks nothing new.
func TestConnectionSuspendsADepartedContext(t *testing.T) {
	cfg := resolving("staging", "key-1") // "prod" is not named

	res, v := connect(t, cfg, values{})

	assert.Equal(t, probe.VerdictSuspended, res.Verdict())
	assert.Equal(t, ReasonContextNotFound, res.Reason())
	assert.True(t, v.departed)
}

// The file still names it, so the remedy is to fix the file — a failure on the retry ladder,
// since nothing here can tell when the user has.
func TestConnectionFailsWhenTheFileWillNotResolve(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.err = errors.New("open ca.crt: no such file")

	res, v := connect(t, cfg, values{departed: true})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonResolveFailed, res.Reason())
	assert.ErrorIs(t, res.Err(), cfg.err)
	assert.False(t, v.departed, "the file names it, so it has not departed")
}

// An unread kubeconfig names nothing, and reporting that as anything would tell every holder
// its context is gone for as long as the first read takes.
func TestConnectionSkipsAnUnreadKubeconfig(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.err = kubeconfig.ErrNotRead

	res, v := connect(t, cfg, values{})

	assert.True(t, res.IsSkip())
	assert.False(t, v.departed)
}

// Scaffolding while nothing dials: resolving is the precondition, not an answer about the
// server, so a context that resolves suspends with ReasonResolved rather than claiming success.
func TestConnectionSuspendsAResolvedContext(t *testing.T) {
	cfg := resolving("prod", "key-1")

	res, v := connect(t, cfg, values{departed: true})

	assert.Equal(t, probe.VerdictSuspended, res.Verdict())
	assert.Equal(t, ReasonResolved, res.Reason())
	assert.False(t, v.departed, "a context that resolves again is back")
}

// Unreachable while nothing dials, and it says so rather than going quiet — a probe that
// suspends without a reason is one nobody can explain.
func TestAnUnimplementedProbeRecordsWhy(t *testing.T) {
	res, commit := unimplemented("readiness")(t.Context(), "prod", values{})

	assert.Equal(t, probe.VerdictSuspended, res.Verdict())
	assert.Equal(t, ReasonInternal, res.Reason())
	assert.Contains(t, res.Message(), "readiness")
	assert.Nil(t, commit)
}

// --- through the engine ---

// The four probes behind the connection are recorded rather than dialed while nothing has
// succeeded at reaching the server — which, while nothing dials, is always.
func TestDependentsRecordDependencyFailedWhileNothingDials(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	watched := lease.WatchState()
	defer watched.Close()

	st := awaitState(t, watched, func(st State) bool {
		return st.ServerUID.LastAttempt.Done()
	})

	assert.Equal(t, ReasonDependencyFailed, st.ServerUID.LastAttempt.Reason)
	assert.False(t, st.ServerUID.Scheduled(), "suspended for the rest of the outage")
	assert.True(t, st.ServerUID.LastAttempt.StartedAt.IsZero(), "recorded, never dispatched")
}

// A context that comes back is re-read and reads as pending again — with no failure streak,
// since a departure was the user's edit, not a fault.
func TestAReturningContextReadsAsPendingAgain(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()

	cfg.rotate("prod", "")
	cfg.changed()
	require.Eventually(t, lease.Departed, time.Second, time.Millisecond)

	cfg.rotate("prod", "key-1")
	cfg.changed()
	require.Eventually(t, func() bool { return !lease.Departed() }, time.Second, time.Millisecond)

	conn := lease.State().Connection
	assert.Equal(t, PhasePending, lease.State().Phase())
	assert.Equal(t, ReasonResolved, conn.LastAttempt.Reason)
	assert.Zero(t, conn.Failures)
}

// A resolve failure keeps its cadence, and consecutive failures are one streak: FailingSince is
// when it began, which the widening ladder means a count alone cannot give.
func TestAResolveFailureKeepsAsking(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.err = errors.New("open ca.crt: no such file")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	watched := lease.WatchState()
	defer watched.Close()

	first := awaitState(t, watched, func(st State) bool {
		return st.Connection.Failures >= 1
	}).Connection
	assert.True(t, first.Scheduled(), "a failure earns a retry")

	// The retry sits out on the ladder; the wake stands in for it, as a worked answer the
	// schedule would eventually produce.
	s.engine.Wake("prod", s.ids.connection)

	second := awaitState(t, watched, func(st State) bool {
		return st.Connection.Failures >= 2
	}).Connection
	assert.Equal(t, first.FailingSince, second.FailingSince, "one run of failures, not two")
}
