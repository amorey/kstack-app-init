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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// connect runs the connection probe's body once, on the test's goroutine, and applies what it
// recorded the way the engine would.
func connect(t *testing.T, cfg *fakeKubeconfig, v connInfo) (probe.Result, connInfo) {
	t.Helper()
	pass := probe.NewPass("prod", v, probe.Snapshot{})
	res := (&connectionProbe{kubecfgSvc: cfg}).Run(t.Context(), pass)
	if next, ok := pass.Updated(); ok {
		v = next
	}
	return res, v
}

// The engine reads a committed value as one that moved and wakes whoever reads it, so a probe
// that found nothing new must hand back nothing — or the four behind it re-run every cycle and
// their intervals stop meaning anything.
func TestTheConnectionProbeCommitsOnlyOnAChange(t *testing.T) {
	cfg := resolving("prod", "key-1")
	_, first := connect(t, cfg, connInfo{departed: true})
	require.False(t, first.departed, "the context resolves, so it is back")

	pass := probe.NewPass("prod", first, probe.Snapshot{})
	res := (&connectionProbe{kubecfgSvc: cfg}).Run(t.Context(), pass)

	assert.Equal(t, ReasonUnreachable, res.Reason())
	_, recorded := pass.Updated()
	assert.False(t, recorded, "nothing moved, so nothing is committed")
}

// Each of the four reads the connection's value to reach the server, so a connection that moves
// re-runs them — even parked, as they are here: nothing answers, so they are suspended behind a
// connection that never succeeds, and only the data edge can reach them.
func TestAMovedConnectionReRunsTheProbesBehindIt(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	startService(t, s)
	lease := s.Acquire("prod")
	defer lease.Release()
	watched := lease.WatchState()
	defer watched.Close()
	before := awaitState(t, watched, func(st State) bool {
		return st.ServerUID.LastAttempt.Done()
	}).ServerUID.LastAttempt

	// A departure moves the connection's value, where a re-read that found the same thing
	// would not.
	cfg.rotate("prod", "")
	cfg.changed()

	after := awaitState(t, watched, func(st State) bool {
		return st.ServerUID.LastAttempt.FinishedAt.After(before.FinishedAt)
	}).ServerUID
	assert.Equal(t, ReasonDependencyFailed, after.LastAttempt.Reason)
	assert.True(t, after.LastAttempt.StartedAt.IsZero(), "re-run, but still never dialed")
}

// --- the connection probe's classifications ---

// A context that left the file has nothing to reach, so its probe suspends — the file is the
// whole truth about presence, and the watch reports it moving, so polling asks nothing new.
func TestConnectionSuspendsADepartedContext(t *testing.T) {
	cfg := resolving("staging", "key-1") // "prod" is not named

	res, v := connect(t, cfg, connInfo{})

	assert.Equal(t, probe.VerdictSuspended, res.Verdict())
	assert.Equal(t, ReasonContextNotFound, res.Reason())
	assert.True(t, v.departed)
}

// The file still names it, so the remedy is to fix the file — a failure on the retry ladder,
// since nothing here can tell when the user has.
func TestConnectionFailsWhenTheFileWillNotResolve(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.err = errors.New("open ca.crt: no such file")

	res, v := connect(t, cfg, connInfo{departed: true})

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

	res, v := connect(t, cfg, connInfo{})

	assert.True(t, res.IsSkip())
	assert.False(t, v.departed)
}

// The whole answer: reached, over TLS we trust, with credentials the server accepts.
func TestConnectionSucceedsWhenTheServerAnswers(t *testing.T) {
	srv := serveAPI(t)
	cfg := serving(srv, "prod", "key-1")

	res, v := connect(t, cfg, connInfo{departed: true})

	assert.Equal(t, probe.VerdictSucceeded, res.Verdict())
	assert.False(t, v.departed, "a context that resolves again is back")
	assert.NotNil(t, v.conn)
	assert.Equal(t, srv.URL, v.endpoint)
	assert.Equal(t, "key-1", v.fingerprint)
}

// A server that will not have these credentials is not an outage: the failure names what the
// user has to fix, and the four probes behind this one could not run either.
func TestConnectionFailsWithWhatTheServerSaid(t *testing.T) {
	srv := serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))

	res, v := connect(t, serving(srv, "prod", "key-1"), connInfo{})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonUnauthorized, res.Reason())
	assert.NotNil(t, v.conn, "the credentials resolved, so the connection stands")
}

func TestConnectionFailsWhenNothingAnswers(t *testing.T) {
	res, _ := connect(t, resolving("prod", "key-1"), connInfo{})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonUnreachable, res.Reason())
}

// A 200 from something that is not an API server is the case a status code cannot catch — a
// captive portal answers everything, so what proves the far end is a Kubernetes API server is
// its body.
func TestConnectionFailsWhenTheAnswerIsNotKubernetes(t *testing.T) {
	srv := serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))

	res, _ := connect(t, serving(srv, "prod", "key-1"), connInfo{})

	assert.Equal(t, probe.VerdictFailed, res.Verdict())
	assert.Equal(t, ReasonMalformed, res.Reason())
}

// Rebuilding a healthy connection would drop every socket under the holders using it, so the
// same credentials keep the same connection — and commit nothing, since the four probes behind
// it wake on a committed value.
func TestConnectionKeepsItsConnectionWhileTheCredentialsHold(t *testing.T) {
	cfg := serving(serveAPI(t), "prod", "key-1")
	_, first := connect(t, cfg, connInfo{})
	require.NotNil(t, first.conn)

	pass := probe.NewPass("prod", first, probe.Snapshot{})
	res := (&connectionProbe{kubecfgSvc: cfg}).Run(t.Context(), pass)

	assert.Equal(t, probe.VerdictSucceeded, res.Verdict())
	_, recorded := pass.Updated()
	assert.False(t, recorded, "nothing moved, so nothing is committed")
}

// Credentials that moved are a different server as far as anything derived from them goes, so
// the connection is rebuilt — and the new pointer is what wakes the probes reading it.
func TestConnectionRebuildsWhenTheCredentialsMove(t *testing.T) {
	cfg := serving(serveAPI(t), "prod", "key-1")
	_, first := connect(t, cfg, connInfo{})
	require.NotNil(t, first.conn)

	cfg.rotate("prod", "key-2")
	res, second := connect(t, cfg, first)

	assert.Equal(t, probe.VerdictSucceeded, res.Verdict())
	assert.NotSame(t, first.conn, second.conn)
	assert.Equal(t, "key-2", second.fingerprint)
}

// A file that will not resolve keeps its connection, where a departure drops one: the read
// failed, which says nothing about whether the credentials behind the connection still work, and
// an editor saving non-atomically is a read that fails for a moment.
func TestConnectionKeepsItsConnectionThroughAResolveFailure(t *testing.T) {
	cfg := serving(serveAPI(t), "prod", "key-1")
	_, first := connect(t, cfg, connInfo{})
	require.NotNil(t, first.conn)

	cfg.setErr(errors.New("open ca.crt: no such file"))
	res, second := connect(t, cfg, first)

	require.Equal(t, ReasonResolveFailed, res.Reason())
	assert.Same(t, first.conn, second.conn)
	select {
	case <-second.conn.Done():
		t.Fatal("the connection was retired over a read that failed")
	default:
	}
}

// A context that left the file has no connection to hand out. Retiring the one it had is the
// pool's, not the probe's — this only stops naming it.
func TestConnectionDropsItsConnectionWhenTheContextDeparts(t *testing.T) {
	cfg := serving(serveAPI(t), "prod", "key-1")
	_, first := connect(t, cfg, connInfo{})
	require.NotNil(t, first.conn)

	cfg.rotate("prod", "")
	_, second := connect(t, cfg, first)

	assert.True(t, second.departed)
	assert.Nil(t, second.conn)
	assert.Empty(t, second.endpoint)
}

// The trap: a build that fails commits the new fingerprint, so a run keyed on the fingerprint
// alone would find it unchanged and never build again — the probe would climb its ladder
// retrying nothing.
func TestConnectionBuildsAgainAfterABuildThatFailed(t *testing.T) {
	srv := serveAPI(t)
	cfg := serving(srv, "prod", "key-1")
	cfg.host = "://nonsense"

	res, failed := connect(t, cfg, connInfo{})
	require.Equal(t, ReasonResolveFailed, res.Reason())
	require.Nil(t, failed.conn)
	require.Equal(t, "key-1", failed.fingerprint, "the fingerprint it tried")

	cfg.host = srv.URL
	res, built := connect(t, cfg, failed)

	assert.Equal(t, probe.VerdictSucceeded, res.Verdict())
	assert.NotNil(t, built.conn, "the fingerprint did not move, but there was no connection")
}

// Unreachable while the connection never succeeds, and it says so rather than going quiet — a
// probe that suspends without a reason is one nobody can explain.
func TestAnUnimplementedProbeRecordsWhy(t *testing.T) {
	pass := probe.NewPass("prod", ComponentStatus{}, probe.Snapshot{})
	res := unimplemented[ComponentStatus]{"readiness"}.Run(t.Context(), pass)

	assert.Equal(t, probe.VerdictSuspended, res.Verdict())
	assert.Equal(t, ReasonInternal, res.Reason())
	assert.Contains(t, res.Message(), "readiness")
	_, recorded := pass.Updated()
	assert.False(t, recorded)
}

// --- through the engine ---

// The four probes behind the connection are recorded rather than dialed while nothing has
// succeeded at reaching the server: one timeout per cycle, not one per probe.
func TestDependentsRecordDependencyFailedWhileTheServerIsUnreachable(t *testing.T) {
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

// A context that comes back is re-read and dialed again — with no failure streak, since a
// departure was the user's edit, not a fault.
func TestAReturningContextIsDialedAgain(t *testing.T) {
	cfg := serving(serveAPI(t), "prod", "key-1")
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

	require.Eventually(t, func() bool {
		return lease.State().Phase() == PhaseProbed
	}, time.Second, time.Millisecond)
	assert.Zero(t, lease.State().Connection.Failures)
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
	s.engine.Wake("prod", nameConnection)

	second := awaitState(t, watched, func(st State) bool {
		return st.Connection.Failures >= 2
	}).Connection
	assert.Equal(t, first.FailingSince, second.FailingSince, "one run of failures, not two")
}

// The engine hands back what a run committed when it drops the value — a commit refused because
// the context was released mid-run, a Skip, a panic. Nothing else can reach that connection: it
// reached neither a snapshot nor an entry.
func TestDiscardRetiresWhatTheRunBuilt(t *testing.T) {
	conn := connTo(t, serveAPI(t))

	(&connectionProbe{}).Discard(connInfo{conn: conn})

	<-conn.Done()
}

// A context that resolves to nothing commits a value with no connection, which is the ordinary
// shape of a dropped one.
func TestDiscardOfAValueWithNoConnectionIsANoOp(t *testing.T) {
	(&connectionProbe{}).Discard(connInfo{departed: true})
}
