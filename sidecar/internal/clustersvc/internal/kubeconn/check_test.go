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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// stateOf reads the entry a test set up, which only a white-box test may do. Safe unlocked: no
// loops are running in the tests that use it.
func stateOf(t *testing.T, s *Service, contextName string) State {
	t.Helper()
	e := s.claimed[contextName]
	require.NotNil(t, e, "no entry for %q", contextName)
	return e.state
}

// awaitKey drains the check queue until want turns up, for a test where more than one check is
// due and the order they land in is not the point.
func awaitKey(t *testing.T, s *Service, want checkKey) {
	t.Helper()
	s.settle()
	for {
		key, ok := s.checkQ.Next(within(t))
		require.True(t, ok, "the queue closed before %v was asked for", want)
		s.checkQ.Done(key)
		if key == want {
			return
		}
	}
}

// drainKeys takes everything the check queue holds once the schedules have settled, giving each
// back. What a worker pool would pick up, without waiting for anything more to arrive.
func drainKeys(t *testing.T, s *Service) []checkKey {
	t.Helper()
	s.settle()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Next takes what is queued and reports closed rather than waiting for more.

	var keys []checkKey
	for {
		key, ok := s.checkQ.Next(ctx)
		if !ok {
			return keys
		}
		s.checkQ.Done(key)
		keys = append(keys, key)
	}
}

// takeKeys drains n keys off the check queue, giving each back so the next ask for it is not
// held behind a run that never happens.
func takeKeys(t *testing.T, s *Service, n int) []checkKey {
	t.Helper()
	s.settle()
	keys := make([]checkKey, 0, n)
	for range n {
		key, ok := s.checkQ.Next(within(t))
		require.True(t, ok, "the queue closed with %d of %d keys taken", len(keys), n)
		s.checkQ.Done(key)
		keys = append(keys, key)
	}
	return keys
}

// --- the connection check ---

// Resolving is the precondition, not the answer: nothing dialed, so no attempt is recorded and
// the phase stays pending. Nothing is scheduled either — there is nothing to poll for until
// something dials, and the file moving is what brings the check back.
func TestAResolvableContextRecordsNoAttemptAndSchedulesNothing(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()

	s.checkConnection("prod")

	st := stateOf(t, s, "prod")
	assert.Equal(t, PhasePending, st.Phase())
	assert.False(t, st.Connection.LastAttempt.Done())
	assert.False(t, st.Connection.Scheduled())
}

// The generation is what makes it due again, so the watch names no check — it says the file
// moved and reconcile works out who cares.
func TestAKubeconfigChangeMakesTheConnectionCheckDue(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()
	require.Equal(t, []checkKey{connectionOf("prod")}, takeKeys(t, s, 1), "the claim's own ask")
	s.checkConnection("prod")

	s.bumpKubeconfig()

	assert.Equal(t, []checkKey{connectionOf("prod")}, takeKeys(t, s, 1))
}

// A context that left the file has nothing to reach, so its check suspends — and records why,
// because LastAttempt.Reason is the only place a suspension's reason lives.
func TestADepartedContextSuspendsItsConnectionCheck(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	defer s.Acquire("prod").Release()

	cfg.rotate("prod", "")
	s.checkConnection("prod")

	conn := stateOf(t, s, "prod").Connection
	assert.Equal(t, ReasonContextNotFound, conn.LastAttempt.Reason)
	assert.False(t, conn.Scheduled(), "the watch reports the file moving; polling asks nothing new")
	assert.Equal(t, 1, conn.Failures)
}

// The file still names it, so the remedy is to fix the file — which means keep asking, since
// nothing here can tell when the user has.
func TestAResolveFailureKeepsAsking(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.err = errors.New("open ca.crt: no such file")
	s := New(cfg)
	defer s.Acquire("prod").Release()

	s.checkConnection("prod")
	first := stateOf(t, s, "prod").Connection
	require.Equal(t, ReasonResolveFailed, first.LastAttempt.Reason)
	assert.True(t, first.Scheduled())
	assert.Equal(t, 1, first.Failures)
	require.False(t, first.FailingSince.IsZero())

	s.checkConnection("prod")

	second := stateOf(t, s, "prod").Connection
	assert.Equal(t, 2, second.Failures)
	assert.Equal(t, first.FailingSince, second.FailingSince, "one run of failures, not two")
}

// A context that comes back is not a server we failed to reach. Leaving the departure recorded
// would report the cluster as probe-failed for good, since nothing else clears it.
func TestAReturningContextReadsAsUnattempted(t *testing.T) {
	cfg := resolving("prod", "key-1")
	s := New(cfg)
	defer s.Acquire("prod").Release()
	cfg.rotate("prod", "")
	s.checkConnection("prod")
	require.Equal(t, PhaseUnreached, stateOf(t, s, "prod").Phase())

	cfg.rotate("prod", "key-1")
	s.bumpKubeconfig()
	s.checkConnection("prod")

	conn := stateOf(t, s, "prod").Connection
	assert.Equal(t, PhasePending, stateOf(t, s, "prod").Phase())
	assert.Equal(t, ReasonUnknown, conn.LastAttempt.Reason)
	assert.Zero(t, conn.Failures)
	assert.True(t, conn.FailingSince.IsZero())
}

// Same for a file the user fixed. This one also stops the retry cadence the failure earned: with
// nothing left failing, there is nothing to keep asking about.
func TestAFixedKubeconfigReadsAsUnattempted(t *testing.T) {
	cfg := failingToResolve()
	s := New(cfg)
	defer s.Acquire("prod").Release()
	s.checkConnection("prod")
	require.True(t, stateOf(t, s, "prod").Connection.Scheduled(), "a failure earns a retry")

	cfg.err = nil
	s.bumpKubeconfig()
	s.checkConnection("prod")

	conn := stateOf(t, s, "prod").Connection
	assert.Equal(t, PhasePending, stateOf(t, s, "prod").Phase())
	assert.Zero(t, conn.Failures)
	assert.False(t, conn.Scheduled())
}

// A read that concluded nothing must leave nothing to derive a retry from. The interval is zero
// so the stale failure's retry is already due — which is what it becomes with any interval, given
// time — and a run that left it in place would come straight back, forever.
func TestAnUnreadKubeconfigLeavesNoRetryToSpinOn(t *testing.T) {
	cfg := failingToResolve()
	s := newWithOptions(cfg, withIntervals(shrunk(checkConnection, 0)))
	defer s.Acquire("prod").Release()
	s.checkConnection("prod")
	require.Equal(t, ReasonResolveFailed, stateOf(t, s, "prod").Connection.LastAttempt.Reason)
	require.NotEmpty(t, drainKeys(t, s), "the retry that failure earned, and the checks it woke")

	cfg.err = kubeconfig.ErrNotRead
	s.checkConnection("prod")

	assert.Empty(t, drainKeys(t, s), "an unread read left work due immediately")
}

// A run moves out of NextAttempt as it completes, so the schedule it was dispatched on survives
// into the record. Without it, StartedAt has nothing to be measured against and a check that
// waited for a worker is indistinguishable from one the server was slow to answer.
func TestARecordedAttemptKeepsTheScheduleItRanOn(t *testing.T) {
	s := New(failingToResolve())
	defer s.Acquire("prod").Release()
	s.settle()
	dueAt := stateOf(t, s, "prod").Connection.NextAttempt.ScheduledAt
	require.False(t, dueAt.IsZero(), "the claim's own check is scheduled")

	s.checkConnection("prod")

	last := stateOf(t, s, "prod").Connection.LastAttempt
	assert.Equal(t, dueAt, last.ScheduledAt)
	assert.False(t, last.StartedAt.Before(dueAt), "a run does not start before it is due")
}

// --- the checks behind it ---

// A server nothing reached cannot answer them, so they are recorded rather than dialed. That is
// what keeps a dead cluster costing one timeout per cycle instead of one per check.
func TestACheckBehindTheConnectionSuspendsWhileItIsDown(t *testing.T) {
	s := New(failingToResolve())
	defer s.Acquire("prod").Release()
	s.checkConnection("prod") // reachability has to answer before anything behind it is due

	s.runCheck(t.Context(), checkKey{contextName: "prod", check: checkServerUID})
	s.settle()

	uid := stateOf(t, s, "prod").ServerUID
	assert.Equal(t, ReasonDependencyFailed, uid.LastAttempt.Reason)
	assert.False(t, uid.Scheduled())
	assert.True(t, uid.LastAttempt.StartedAt.IsZero(), "recorded, never dispatched")
}

// A suspended check has no timer left to fire, so nothing but the connection could ask for it
// again. This is the whole re-arm path.
func TestAConnectionComingUpArmsWhatSuspendedOnIt(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()
	require.Equal(t, []checkKey{connectionOf("prod")}, takeKeys(t, s, 1), "the claim's own ask")
	s.checkConnection("prod") // reads the file, so only the four below are left due
	for id, c := range checks {
		if c.needsConnection {
			dependencyFailed(checkID(id))(s.claimed["prod"]) // the outage they suspended on
		}
	}

	s.commitCheck(connectionOf("prod"), s.claimed["prod"], func(e *entry) {
		observations(&e.state)[checkConnection].record(succeededAt(time.Now()))
	})

	armed := map[checkID]bool{}
	for _, key := range takeKeys(t, s, 4) {
		armed[key.check] = true
	}
	assert.Equal(t, map[checkID]bool{
		checkReadiness: true, checkServerUID: true, checkServerVersion: true, checkPrincipal: true,
	}, armed)
}

// Every check behind the connection declares the dependency, so none is left dialing a server
// the connection check has already found unreachable.
func TestEveryCheckButTheConnectionDependsOnIt(t *testing.T) {
	for id, c := range checks {
		assert.Equal(t, checkID(id) != checkConnection, c.needsConnection, "%v", checkID(id))
	}
}

// Unreachable while nothing dials, and it says so rather than going quiet — a check that
// suspends without a reason is one nobody can explain.
func TestAnUnimplementedCheckRecordsWhy(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()
	key := checkKey{contextName: "prod", check: checkReadiness}

	s.commitCheck(key, s.claimed["prod"], checks[key.check].run(t.Context(), checkArgs{svc: s, contextName: key.contextName}))
	s.settle()

	readiness := stateOf(t, s, "prod").Readiness
	assert.Equal(t, ReasonInternal, readiness.LastAttempt.Reason)
	assert.False(t, readiness.Scheduled())
}

// --- what due decides ---

// dueFor is what the scheduler decides for one check of an entry a test built by hand.
func dueFor(s *Service, e *entry, id checkID) time.Time {
	return s.due(e, id, observations(&e.state)[id], time.Now())
}

// connected is an entry whose reachability check answered, with the kubeconfig already read.
func connected(s *Service) *entry {
	e := &entry{kubecfgGen: s.kubecfgGen}
	observations(&e.state)[checkConnection].record(succeededAt(runAt))
	return e
}

// One run records DependencyFailed and the rest of the outage costs nothing — the other half of
// keeping a dead cluster at one timeout per cycle.
func TestADependentStaysSuspendedForTheRestOfAnOutage(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	e := &entry{kubecfgGen: s.kubecfgGen}
	observations(&e.state)[checkConnection].record(Attempt{FinishedAt: runAt, Reason: ReasonUnreachable})
	observations(&e.state)[checkServerUID].record(Attempt{FinishedAt: runAt, Reason: ReasonDependencyFailed})

	assert.True(t, dueFor(s, e, checkServerUID).IsZero())
}

// The endpoint is absent rather than failing, so a connection that is up does not make it worth
// asking again.
func TestAnUnsupportedCheckStaysSuspendedWhileTheConnectionIsUp(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	e := connected(s)
	observations(&e.state)[checkReadiness].record(Attempt{FinishedAt: runAt, Reason: ReasonUnsupported})

	assert.True(t, dueFor(s, e, checkReadiness).IsZero())
}

// A check that has never run is due as soon as there is a connection to run it over.
func TestACheckThatNeverRanIsDueOnceTheConnectionIsUp(t *testing.T) {
	s := New(resolving("prod", "key-1"))

	assert.False(t, dueFor(s, connected(s), checkServerUID).IsZero())
}

// A run in flight is not rescheduled at all — reconcile leaves it alone rather than asking due,
// because NextAttempt is the run and its commit is what decides what comes after.
func TestReconcileLeavesAnInFlightCheckUntouched(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()
	s.settle()
	e := s.claimed["prod"]
	observations(&e.state)[checkServerUID].begin(runAt)

	s.reconcile("prod")

	assert.Equal(t, runAt, e.state.ServerUID.NextAttempt.StartedAt)
	assert.True(t, e.state.ServerUID.InFlight())
}

// An ordinary failure is neither terminal nor a dependency's, so the check keeps its cadence.
func TestAnOrdinaryFailureKeepsTheChecksCadence(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	e := connected(s)
	observations(&e.state)[checkServerUID].record(Attempt{FinishedAt: runAt, Reason: ReasonForbidden})

	assert.Equal(t, runAt.Add(defaultIntervals[checkServerUID]), dueFor(s, e, checkServerUID))
}

// Nothing below reachability runs before reachability has answered: there is nothing to say
// about a server nobody has tried.
func TestADependentIsNotDueBeforeTheConnectionHasAnswered(t *testing.T) {
	s := New(resolving("prod", "key-1"))

	assert.True(t, dueFor(s, &entry{kubecfgGen: s.kubecfgGen}, checkServerUID).IsZero())
}

// A queued key outlives the claim that asked for it: the last holder can release and another
// caller re-claim the name before a worker gets there. The replacement has reached no server, so
// it never scheduled this check — recording against it would show a dependency failure for a
// connection nobody has tried.
func TestAQueuedCheckIsDroppedWhenItsClaimIsReplaced(t *testing.T) {
	uidCheck := checkKey{contextName: "prod", check: checkServerUID}
	s := New(failingToResolve())
	lease := s.Acquire("prod")
	s.checkConnection("prod") // reachability answers, so the checks behind it fall due
	require.Contains(t, drainKeys(t, s), uidCheck)

	lease.Release()
	defer s.Acquire("prod").Release()
	s.settle()
	s.runCheck(t.Context(), uidCheck)
	s.settle()

	uid := stateOf(t, s, "prod").ServerUID
	assert.Equal(t, ReasonUnknown, uid.LastAttempt.Reason)
	assert.Zero(t, uid.Failures)
	assert.False(t, uid.InFlight(), "a dropped run must not leave the check marked out")
}

// A reconcile can land while a check is out — another check committing, a kubeconfig change, a
// timer. NextAttempt is that run, so overwriting it erases the mark saying the run is still going
// and the schedule it was dispatched on.
func TestAReconcileLeavesAnInFlightRunAlone(t *testing.T) {
	cfg := failingToResolve()
	s := New(cfg)
	defer s.Acquire("prod").Release()
	s.settle()
	dueAt := stateOf(t, s, "prod").Connection.NextAttempt.ScheduledAt

	var inFlight bool
	cfg.duringRead(func() {
		s.reconcile("prod")
		inFlight = s.claimed["prod"].state.Connection.InFlight()
	})
	s.checkConnection("prod")

	assert.True(t, inFlight, "the run stopped reading as in flight while it was still going")
	assert.Equal(t, dueAt, stateOf(t, s, "prod").Connection.LastAttempt.ScheduledAt,
		"the schedule the run was dispatched on")
}

// --- scheduling ---

// The schedule reconcile derives is what brings the pass back. No workers here on purpose: the
// only thing left to ask is the timer.
func TestAScheduledCheckAsksForAReconcileWhenItIsDue(t *testing.T) {
	s := newWithOptions(failingToResolve(), withIntervals(shrunk(checkConnection, time.Millisecond)))
	defer s.Acquire("prod").Release()
	s.checkConnection("prod") // a failure is what earns a retry
	require.True(t, stateOf(t, s, "prod").Connection.Scheduled())

	contextName, ok := s.reconcileQ.Next(within(t))

	require.True(t, ok)
	assert.Equal(t, "prod", contextName)
}

// An entry nobody holds is one nothing checks, so the last release gives up its schedule.
func TestReleaseStopsTheScheduledRuns(t *testing.T) {
	s := New(failingToResolve())
	lease := s.Acquire("prod")
	s.checkConnection("prod")
	e := s.claimed["prod"]
	require.NotNil(t, e.timer)

	lease.Release()

	assert.Nil(t, e.timer)
}

func TestCloseStopsTheScheduledRuns(t *testing.T) {
	s := New(failingToResolve())
	defer s.Acquire("prod").Release()
	s.checkConnection("prod")
	e := s.claimed["prod"]
	require.NotNil(t, e.timer)

	require.NoError(t, s.Close())

	assert.Nil(t, e.timer)
}

// --- what a run publishes ---

// The timing moves every run. Signalling on it would wake every cluster's reconcile on every
// cadence to find nothing changed.
func TestARunThatChangedNothingIsNotAnnounced(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	defer s.Acquire("prod").Release()
	s.checkConnection("prod")
	news := s.Subscribe()
	defer news.Close()

	s.checkConnection("prod")

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := news.RecvContext(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// A claim watcher gets it anyway: the timing is what it subscribed for, and the countdown to the
// next run is only visible here.
func TestARunThatChangedNothingStillReachesAClaimWatcher(t *testing.T) {
	s := New(failingToResolve())
	lease := s.Acquire("prod")
	defer lease.Release()
	s.checkConnection("prod")
	watched := lease.WatchState()
	defer watched.Close()

	s.checkConnection("prod") // same answer, so no news — but a new countdown

	ev, err := watched.RecvContext(within(t))
	require.NoError(t, err)
	assert.Equal(t, ReasonResolveFailed, ev.Value.Connection.LastAttempt.Reason)
	assert.True(t, ev.Value.Connection.Scheduled())
}

// --- recordAttempt ---

func TestRecordAttemptKeepsTheValueThroughAFailure(t *testing.T) {
	o := Observation[string]{Value: "uid-1", LastSeen: runAt}

	o.record(Attempt{FinishedAt: runAt, Reason: ReasonForbidden})

	assert.Equal(t, "uid-1", o.Value, "a read that stopped being permitted is not a fact that moved")
	assert.Equal(t, runAt, o.LastSeen)
	assert.Equal(t, 1, o.Failures)
	assert.Equal(t, runAt, o.FailingSince)
}

func TestRecordAttemptClearsTheFailureRunOnSuccess(t *testing.T) {
	o := Observation[string]{Attempts: Attempts{Failures: 3, FailingSince: runAt}}

	o.record(succeededAt(runAt))

	assert.Zero(t, o.Failures)
	assert.True(t, o.FailingSince.IsZero())
}

// A cancellation says nothing about the cluster, so it moves neither failure field.
func TestRecordAttemptIgnoresACancellation(t *testing.T) {
	o := Observation[string]{Attempts: Attempts{Failures: 2, FailingSince: runAt}}

	o.record(Attempt{FinishedAt: runAt, Reason: ReasonCanceled})

	assert.Equal(t, 2, o.Failures)
	assert.Equal(t, runAt, o.FailingSince)
}

// --- the per-check index ---

// Each entry of the index reaches exactly one observation, so a check can never write another's
// answer.
func TestEachCheckReachesOneObservation(t *testing.T) {
	for id := range numChecks {
		var st State
		observations(&st)[id].record(Attempt{FinishedAt: runAt, Reason: ReasonForbidden})

		answered := 0
		for _, a := range observations(&st) {
			if a.LastAttempt.Done() {
				answered++
			}
		}
		assert.Equal(t, 1, answered, "%v wrote %d observations", id, answered)
		assert.True(t, observations(&st)[id].LastAttempt.Done(), "%v", id)
	}
}

func TestAnUnknownCheckIsNamedByNumber(t *testing.T) {
	assert.Equal(t, "check(5)", checkID(numChecks).String())
}

func TestEveryCheckIsNamed(t *testing.T) {
	assert.Equal(t,
		[]string{"connection", "readiness", "serverUID", "serverVersion", "principal"},
		[]string{
			checkConnection.String(), checkReadiness.String(), checkServerUID.String(),
			checkServerVersion.String(), checkPrincipal.String(),
		})
}

// A queued reconcile can outlive the claim that asked for it — the last holder released between
// the ask and the pass. It finds no entry and schedules nothing, rather than reviving a context
// nobody holds.
func TestAReconcileForAnUnclaimedContextSchedulesNothing(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	s.Acquire("prod").Release()

	s.settle()

	assert.Empty(t, s.claimed)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, ok := s.checkQ.Next(ctx)
	assert.False(t, ok, "a check was asked for on behalf of nobody")
}

// A timer can outlive the claim it was armed for. The run finds no entry and writes nothing,
// rather than resurrecting a context nobody holds.
func TestARunForAnUnclaimedContextDoesNothing(t *testing.T) {
	cfg := resolving("prod", "key-1")
	cfg.reads = testutil.NewProbe[string](2)
	s := New(cfg)

	s.checkConnection("prod")

	assert.Empty(t, s.claimed)
	assert.Empty(t, cfg.reads.Chan(), "the kubeconfig was not even read")
}

// failingToResolve is a kubeconfig that names "prod" and will not yield credentials for it —
// the one answer a check can reach without a server that leaves something scheduled.
func failingToResolve() *fakeKubeconfig {
	cfg := resolving("prod", "key-1")
	cfg.err = errors.New("open ca.crt: no such file")
	return cfg
}

// succeededAt is an attempt that answered, which is what OK reads.
func succeededAt(at time.Time) Attempt {
	return Attempt{StartedAt: at, FinishedAt: at, Reason: ReasonSucceeded}
}

// shrunk paces one check for a test and leaves the rest at their production value.
func shrunk(id checkID, d time.Duration) [numChecks]time.Duration {
	intervals := defaultIntervals
	intervals[id] = d
	return intervals
}
