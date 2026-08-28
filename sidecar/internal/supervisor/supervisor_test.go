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

package supervisor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// subj is the one subject most tests track.
const subj = "ctx-1"

// steered is a reconciler the test steers: the result is swapped at will, runs are counted, and an
// optional hook fires inside the run — which is how a test interleaves a Remove or a Wake with
// a run in flight.
type steered struct {
	mu          sync.Mutex
	res         Result
	next        *string
	onRun       func()
	runs        int
	sawDeadline bool
}

func (p *steered) Reconcile(ctx context.Context, pass *Pass[string]) Result {
	p.mu.Lock()
	p.runs++
	_, p.sawDeadline = ctx.Deadline()
	res, next, hook := p.res, p.next, p.onRun
	p.mu.Unlock()

	if hook != nil {
		hook()
	}
	if next != nil {
		pass.Commit(*next)
	}
	return res
}

func (p *steered) set(res Result) { p.mu.Lock(); defer p.mu.Unlock(); p.res = res }
func (p *steered) count() int     { p.mu.Lock(); defer p.mu.Unlock(); return p.runs }

// reconcilerFunc adapts a bare func for a test that needs a one-off body.
type reconcilerFunc func(ctx context.Context, pass *Pass[string]) Result

func (f reconcilerFunc) Reconcile(ctx context.Context, pass *Pass[string]) Result {
	return f(ctx, pass)
}

// single is an supervisor with one steered reconciler, closed on cleanup.
func single(t *testing.T, res Result, opts ...ReconcilerOption) (*Supervisor, *steered, string) {
	t.Helper()
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	p := &steered{res: res}
	Register(e, "conn", p, opts...)
	return e, p, "conn"
}

// pair is single plus a reconciler requiring the first.
func pair(t *testing.T, aRes, bRes Result) (e *Supervisor, a, b *steered, aName, bName string) {
	t.Helper()
	e = New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	a, b = &steered{res: aRes}, &steered{res: bRes}
	Register(e, "conn", a)
	Register(e, "uid", b, WithDependencies("conn"))
	return e, a, b, "conn", "uid"
}

// linked is a pair where the second declares both edges on the first — the shape kubeconn's
// watchers have, and the only one where a value change and a health gate are both in play.
func linked(t *testing.T, aRes, bRes Result) (e *Supervisor, a, b *steered, aName, bName string) {
	t.Helper()
	e = New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	a, b = &steered{res: aRes}, &steered{res: bRes}
	Register(e, "conn", a)
	Register(e, "uid", b, WithDependencies("conn"), WithWatches("conn"))
	return e, a, b, "conn", "uid"
}

// commits makes the body hand back v, which the supervisor reads as "the value moved".
func (p *steered) commits(v string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = &v
}

// commitsNothing makes the body hand back nil — the contract for an answer that did not move.
func (p *steered) commitsNothing() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = nil
}

// settled runs both reconcilers of a linked pair to a succeeded rest, so what follows is measured
// against two reconcilers parked on their intervals rather than against a fresh subject's first pass.
func settled(t *testing.T, e *Supervisor) {
	t.Helper()
	e.Add(subj)
	e.settle()
	runNext(t, e)
	runNext(t, e)
	noRuns(t, e, "the pair should be parked on its interval")
}

// within bounds a wait for something that must arrive, so a regression fails the test rather
// than hanging it until the suite's own deadline.
func within(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// settle runs every pass currently queued, on the test's goroutine — where passLoop would pick
// them up.
func (e *Supervisor) settle() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Next takes what is queued and reports rather than waiting for more.

	for {
		name, ok := e.passQ.Next(ctx)
		if !ok {
			return
		}
		e.pass(name)
		e.passQ.Done(name)
	}
}

// runNext takes the next due run and executes it the way a worker would, then settles the pass
// it asked for. All on the test's goroutine, so assertions read settled state.
func runNext(t *testing.T, e *Supervisor) key {
	t.Helper()
	k, ok := e.runQ.Next(within(t))
	require.True(t, ok, "the run queue closed")
	e.runReconciler(t.Context(), k)
	e.runQ.Done(k)
	e.settle()
	return k
}

// takeRun pops one due run without executing it, giving the key back.
func takeRun(t *testing.T, e *Supervisor) key {
	t.Helper()
	k, ok := e.runQ.Next(within(t))
	require.True(t, ok, "the run queue closed")
	e.runQ.Done(k)
	return k
}

// drainRuns empties the run queue, so a following negative assertion answers to what the test
// did rather than to what the subject already owed.
func drainRuns(t *testing.T, e *Supervisor) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Next takes what is queued and reports rather than waiting for more.
	for {
		k, ok := e.runQ.Next(ctx)
		if !ok {
			return
		}
		e.runQ.Done(k)
	}
}

// noRuns is a negative assertion, so it needs a bounded window rather than the failsafe: it
// fails the instant a run is handed out.
func noRuns(t *testing.T, e *Supervisor, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if k, ok := e.runQ.Next(ctx); ok {
		e.runQ.Done(k)
		t.Fatalf("%s: %v was handed out", msg, k)
	}
}

// keyOf is the run-queue key for a reconciler the test names, which is what the queue is keyed by
// even though a name is what a caller addresses.
func keyOf(e *Supervisor, name string) key {
	return key{subject: subj, reconciler: e.byName[name]}
}

// att reads one reconciler's attempts through Read.
func att(t *testing.T, e *Supervisor, name string) Attempts {
	t.Helper()
	v, ok := e.Read(subj)
	require.True(t, ok, "subject not tracked")
	return v.Attempts(name)
}

func startSupervisor(t *testing.T, e *Supervisor) {
	t.Helper()
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
}

// --- the backoff ladder ---

func TestBackoffClimbsToItsCap(t *testing.T) {
	b := Backoff{Base: time.Second, Factor: 2, Cap: time.Minute}
	assert.Equal(t, time.Second, b.Delay(1), "the first failure waits the base")
	assert.Equal(t, 2*time.Second, b.Delay(2))
	assert.Equal(t, 8*time.Second, b.Delay(4), "each rung widens by the factor")
	assert.Equal(t, time.Minute, b.Delay(7), "until the cap")
	assert.Equal(t, time.Minute, b.Delay(20), "and stays there")
}

// --- registration ---

// The supervisor's own knobs are options too, and a New that states none takes the defaults.
func TestWithWorkersSetsTheRunWorkerCount(t *testing.T) {
	assert.Equal(t, 3, New(WithWorkers(3)).settings.workers)
	assert.Positive(t, New().settings.workers, "a default fleet")
}

// A supervisor with no workers drains nothing: every subject queues and no run is ever
// dispatched, with no error, no log and no verdict to read. It is a table wired wrong at boot —
// a settings struct built without the field — so it is refused the way every other one is.
func TestASupervisorWithNoWorkersIsRefused(t *testing.T) {
	assert.PanicsWithValue(t, "supervisor: WithWorkers needs at least one worker",
		func() { New(WithWorkers(0)) })
	assert.Panics(t, func() { New(WithWorkers(-1)) })
}

// Register needs something to run, and something to call it.
func TestRegisterPanicsWithoutAReconcilerOrAName(t *testing.T) {
	e := New()

	assert.Panics(t, func() { Register[string](e, "conn", nil) })
	assert.Panics(t, func() { Register(e, "", &steered{}) })
}

// OnPass is wiring, set before the supervisor runs — like a registration.
func TestOnPassPanicsAfterStart(t *testing.T) {
	e, _, _ := single(t, Skip())
	startSupervisor(t, e)

	assert.Panics(t, func() { e.OnPass(func(string, Snapshot) {}) })
}

// A reconciler's public identity is its name; the index behind it is the supervisor's own.
func TestRegisterResolvesAnEdgeToTheReconcilerItNames(t *testing.T) {
	e, _, a := single(t, Skip())
	Register(e, "uid", &steered{}, WithDependencies(a))

	assert.Equal(t, "uid", e.specs[1].name)
	assert.Equal(t, []reconcilerID{0}, e.specs[1].dependencies)
	assert.Equal(t, reconcilerID(1), e.byName["uid"])
}

func TestRegisterPanicsOnADuplicateName(t *testing.T) {
	e, p, _ := single(t, Skip())

	assert.Panics(t, func() { Register(e, "conn", p) })
}

// An edge resolves against what is registered so far, so a forward reference cannot be
// expressed and the registration order stays topological.
func TestRegisterPanicsOnADependencyNotYetRegistered(t *testing.T) {
	e := New()

	assert.Panics(t, func() { Register(e, "uid", &steered{}, WithDependencies("nope")) })
}

// A registration states only what deviates from the package defaults.
func TestRegistrationDefaultsWhatItDoesNotState(t *testing.T) {
	e, _, name := single(t, Skip(), WithInterval(10*time.Minute))

	cfg := e.specs[e.byName[name]].cfg
	assert.Equal(t, 10*time.Minute, cfg.interval)
	assert.Equal(t, defaultCfg.timeout, cfg.timeout)
	assert.Equal(t, defaultCfg.backoff, cfg.backoff)
}

func TestRegisterPanicsAfterStart(t *testing.T) {
	e, p, _ := single(t, Skip())
	startSupervisor(t, e)

	assert.Panics(t, func() { Register(e, "late", p) })
}

// A subject's bookkeeping is sized when it is added, so the reconciler set must be complete first.
func TestRegisterPanicsAfterAdd(t *testing.T) {
	e, p, _ := single(t, Skip())
	e.Add(subj)

	assert.Panics(t, func() { Register(e, "late", p) })
}

// --- subjects and the pass ---

// Removing what was never added, passing over it, and dispatching against it are all no-ops: a
// subject can go while work naming it is still queued.
func TestASubjectNothingTracksIsANoOpEverywhere(t *testing.T) {
	e, p, _ := single(t, Succeeded())

	e.Remove("never-added")
	e.pass("never-added")
	e.runReconciler(t.Context(), key{subject: "never-added"})

	assert.Zero(t, p.count(), "a run was dispatched against a subject nothing tracks")
	noRuns(t, e, "a pass over a subject nothing tracks queued work")
}

// A fresh subject owes exactly its runnable reconcilers: the dependent waits on an answer, and a
// second Add of the same name asks for nothing.
func TestAddQueuesWhatAFreshSubjectOwes(t *testing.T) {
	e, _, _, aID, _ := pair(t, Skip(), Skip())

	e.Add(subj)
	e.settle()

	assert.Equal(t, keyOf(e, aID), takeRun(t, e))
	noRuns(t, e, "more than the connection reconciler was queued")

	e.Add(subj)
	e.settle()
	noRuns(t, e, "a second Add asked for a run")
}

func TestWorkQueuedBeforeStartIsServedOnceItRuns(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	ran := testutil.NewProbe[struct{}](1)
	Register(e, "conn", reconcilerFunc(func(context.Context, *Pass[string]) Result {
		ran.Fire(struct{}{})
		return Suspend("Resolved", "")
	}))
	e.Add(subj)

	startSupervisor(t, e)

	ran.Await(t, "the run a pre-Start Add asked for")
}

func TestRemoveStopsTheSubjectsTimer(t *testing.T) {
	e, _, _ := single(t, Fail("Unreachable", assert.AnError))
	e.Add(subj)
	e.settle()
	runNext(t, e) // a failure earns a retry, which arms the timer
	sub := e.subjects[subj]
	require.NotNil(t, sub.timer)

	e.Remove(subj)

	assert.Nil(t, sub.timer)
	_, ok := e.Read(subj)
	assert.False(t, ok)
}

// The last holder can remove mid-run and another caller re-add the same name. The subject it
// gets is a different one, and a run that predates it commits nothing against it.
func TestARunAgainstARemovedSubjectCommitsNothing(t *testing.T) {
	e, p, id := single(t, Fail("Unreachable", assert.AnError))
	p.onRun = func() {
		e.Remove(subj)
		e.Add(subj)
	}
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, id)
	assert.False(t, a.LastAttempt.Done(), "the stale run reached the replacement subject")
	assert.Zero(t, a.Failures)
}

// --- wakes ---

// A name nothing was registered under is a wiring bug; a subject nothing tracks is not, since a
// claim can be released while a wake for it is in flight.
func TestWakeIgnoresAnUntrackedSubjectAndPanicsOnAnUnknownReconciler(t *testing.T) {
	e, _, id := single(t, Succeeded())
	e.Add(subj)
	e.settle()
	drainRuns(t, e)

	e.Wake("nobody-tracks-this", id)
	noRuns(t, e, "a wake reached a subject nothing tracks")

	assert.Panics(t, func() { e.Wake(subj, "nope") })
}

// A Wake landing mid-run is redelivered when that run commits: the run in flight had already
// passed the thing the Wake is about.
func TestAWakeMidRunEarnsAFreshRun(t *testing.T) {
	e, p, id := single(t, Suspend("ContextNotFound", ""))
	woke := false
	p.onRun = func() {
		if !woke {
			woke = true
			e.Wake(subj, id)
		}
	}
	e.Add(subj)
	e.settle()

	runNext(t, e)

	assert.Equal(t, keyOf(e, id), takeRun(t, e), "the mid-run Wake was folded into the run")
}

// A Wake overrides suspension — it is the one input the derivation cannot produce.
func TestAWakeRunsASuspendedReconciler(t *testing.T) {
	e, _, id := single(t, Suspend("ContextNotFound", ""))
	e.Add(subj)
	e.settle()
	runNext(t, e)
	noRuns(t, e, "a suspended reconciler was scheduled")

	e.Wake(subj, id)

	assert.Equal(t, keyOf(e, id), takeRun(t, e))
}

func TestWakeAllReachesEveryTrackedSubject(t *testing.T) {
	e, _, id := single(t, Suspend("ContextNotFound", ""))
	e.Add("ctx-1")
	e.Add("ctx-2")
	e.settle()
	seen := []key{runNext(t, e), runNext(t, e)} // the fresh subjects' own runs
	require.ElementsMatch(t, seen, []key{{"ctx-1", e.byName[id]}, {"ctx-2", e.byName[id]}})

	e.WakeAll(id)

	assert.ElementsMatch(t,
		[]key{takeRun(t, e), takeRun(t, e)},
		[]key{{"ctx-1", e.byName[id]}, {"ctx-2", e.byName[id]}})
}

// --- the schedule ---

// NextAttempt is the run in flight; writing a schedule over it would erase both the in-flight
// mark and the schedule it was dispatched on. Its commit passes again.
func TestARunInFlightIsLeftAlone(t *testing.T) {
	e, _, id := single(t, Skip())
	e.Add(subj)
	e.settle()
	require.Equal(t, keyOf(e, id), takeRun(t, e), "the fresh subject's own run")
	// The worker that took it marks the run before dropping the lock. Safe unlocked here: no
	// loops are running.
	e.subjects[subj].obs[e.byName[id]].begin(runAt)

	e.pass(subj)

	a := att(t, e, id)
	assert.Equal(t, runAt, a.NextAttempt.StartedAt, "the pass wrote over the run in flight")
	noRuns(t, e, "a second run of a reconciler already out")
}

// The success-path poll is the default; Suspend is the only opt-out.
func TestASucceededReconcilerIsDueAgainAfterItsInterval(t *testing.T) {
	e, _, id := single(t, Succeeded(), WithInterval(250*time.Millisecond))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, id)
	require.True(t, a.OK())
	assert.Equal(t, a.LastAttempt.FinishedAt.Add(250*time.Millisecond), a.NextAttempt.ScheduledAt)
}

// A run that knows it should come back sooner says so, and the registration goes on bounding
// every run that does not.
func TestASucceededRunCanAskToComeBackSooner(t *testing.T) {
	e, _, id := single(t, Succeeded().RequeueAfter(50*time.Millisecond), WithInterval(250*time.Millisecond))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, id)
	assert.Equal(t, a.LastAttempt.FinishedAt.Add(50*time.Millisecond), a.NextAttempt.ScheduledAt)
}

// The registration is the correctness bound, so a run asking for longer is ignored rather than
// obeyed: forget the ask and a subject is slower, never wrong.
func TestASucceededRunCannotAskToComeBackLater(t *testing.T) {
	e, _, id := single(t, Succeeded().RequeueAfter(time.Hour), WithInterval(250*time.Millisecond))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, id)
	assert.Equal(t, a.LastAttempt.FinishedAt.Add(250*time.Millisecond), a.NextAttempt.ScheduledAt)
}

// The ladder widens per consecutive failure and holds at the cap — and it is a pure function of
// Failures, so a later pass derives the same ScheduledAt it derived before.
func TestAFailedReconcilerClimbsTheLadder(t *testing.T) {
	e, _, id := single(t, Fail("Unreachable", assert.AnError),
		WithBackoff(10*time.Millisecond, 2, 40*time.Millisecond))
	e.Add(subj)
	e.settle()

	for _, want := range []time.Duration{10, 20, 40, 40} {
		if att(t, e, id).LastAttempt.Done() {
			e.Wake(subj, id) // the scheduled retry is in the future; force it
		}
		runNext(t, e)

		a := att(t, e, id)
		assert.Equal(t, want*time.Millisecond, a.NextAttempt.ScheduledAt.Sub(a.LastAttempt.FinishedAt))

		e.passQ.Add(subj)
		e.settle()
		assert.Equal(t, a.NextAttempt.ScheduledAt, att(t, e, id).NextAttempt.ScheduledAt,
			"a second pass moved ScheduledAt over unchanged state")
	}
}

// Skip leaves no record — the previous answer stands — and nothing scheduled: only a Wake
// brings the reconciler back, so an unreadable source does not become a busy loop.
func TestASkipLeavesNoRecordAndNothingScheduled(t *testing.T) {
	e, p, id := single(t, Fail("ResolveFailed", assert.AnError))
	e.Add(subj)
	e.settle()
	runNext(t, e)
	failed := att(t, e, id).LastAttempt
	require.True(t, failed.Done())

	p.set(Skip())
	e.Wake(subj, "conn")
	runNext(t, e)

	a := att(t, e, id)
	assert.Equal(t, failed, a.LastAttempt, "a Skip must leave the last record standing")
	assert.False(t, a.Scheduled())
	noRuns(t, e, "a skipped reconciler was re-dispatched")
}

// The timer only brings the pass back; the pass decides again per reconciler.
func TestTheTimerIsAWakeNotACadence(t *testing.T) {
	e, _, _ := single(t, Fail("Unreachable", assert.AnError),
		WithBackoff(time.Millisecond, 2, 2*time.Millisecond))
	e.Add(subj)
	e.settle()

	runNext(t, e) // the failure schedules a retry, which arms the timer

	name, ok := e.passQ.Next(within(t))
	require.True(t, ok, "the timer never asked for a pass")
	e.passQ.Done(name)
	assert.Equal(t, subj, name)
}

// --- dependencies ---

// Nothing to say about a server nobody has tried: the dependent stays untouched rather than
// recording a dependency that has not failed.
func TestADependentIsUntouchedBeforeItsDependenciesAnswer(t *testing.T) {
	e, _, _, aID, bID := pair(t, Skip(), Succeeded())
	e.Add(subj)
	e.settle()

	require.Equal(t, keyOf(e, aID), takeRun(t, e))
	b := att(t, e, bID)
	assert.False(t, b.LastAttempt.Done())
	assert.False(t, b.Scheduled())
}

// One run records DependencyFailed — never dialed, so no StartedAt — and the rest of the outage
// costs nothing: one timeout per cycle, not one per reconciler.
func TestADependentRecordsDependencyFailedOnceThenSuspends(t *testing.T) {
	e, _, bBody, aID, bID := pair(t, Fail("Unreachable", assert.AnError), Succeeded())
	e.Add(subj)
	e.settle()
	require.Equal(t, keyOf(e, aID), runNext(t, e), "the connection answers first")

	require.Equal(t, keyOf(e, bID), runNext(t, e), "the failure makes the dependent due once")

	b := att(t, e, bID)
	assert.Equal(t, VerdictSuspended, b.LastAttempt.Verdict)
	assert.Equal(t, ReasonDependencyFailed, b.LastAttempt.Reason)
	assert.True(t, b.LastAttempt.StartedAt.IsZero(), "recorded, never dispatched")
	assert.Zero(t, b.Failures, "the dependency carries the streak, not the dependent")
	assert.Zero(t, bBody.count(), "the body must not have run")

	e.Wake(subj, aID)
	runNext(t, e) // the outage continues
	noRuns(t, e, "the dependent was recorded again for the same outage")
}

// A suspended dependent has nothing scheduled, so the recovery re-arm is read off the state —
// nothing has to notice it and go looking for what suspended on it.
func TestARecoveredDependencyMakesItsDependentsDue(t *testing.T) {
	e, aBody, _, aID, bID := pair(t, Fail("Unreachable", assert.AnError), Succeeded())
	e.Add(subj)
	e.settle()
	runNext(t, e) // connection fails
	runNext(t, e) // dependent records DependencyFailed

	aBody.set(Succeeded())
	e.Wake(subj, aID)
	runNext(t, e) // connection recovers

	assert.Equal(t, keyOf(e, bID), takeRun(t, e))
}

// A dependency that failed between the pass and the worker picking the key up means the run is
// recorded, not dialed — a worker must not spend a timeout learning what the state already says.
func TestDependenciesAreRecheckedAtDispatch(t *testing.T) {
	e, aBody, bBody, aID, bID := pair(t, Succeeded(), Succeeded())
	e.Add(subj)
	e.settle()
	runNext(t, e) // connection succeeds; the dependent is now queued

	aBody.set(Fail("Unreachable", assert.AnError))
	e.runReconciler(t.Context(), keyOf(e, aID)) // the connection dies before b dispatches
	e.settle()
	require.Equal(t, keyOf(e, bID), runNext(t, e))

	assert.Zero(t, bBody.count(), "the body dialed a server the state already said was down")
	assert.Equal(t, ReasonDependencyFailed, att(t, e, bID).LastAttempt.Reason)
}

// A wake can outrun the pass and dispatch a reconciler whose dependency has never answered. The run
// ends as a no-op — nothing to say about a server nobody has tried — and the pass owns the
// question again.
func TestARunDispatchedBeforeItsDependencyAnsweredRecordsNothing(t *testing.T) {
	e, _, b, _, bName := pair(t, Skip(), Succeeded())
	e.Add(subj)
	e.settle()
	drainRuns(t, e)

	e.Wake(subj, bName)
	runNext(t, e)

	assert.Zero(t, b.count(), "the body ran with nothing to run over")
	at := att(t, e, bName)
	assert.False(t, at.LastAttempt.Done(), "an untouched reconciler records nothing")
}

// --- the data edge ---

// The gap the data edge closes: a dependent parked on its interval goes on using a value that
// moved under it, because health did not change and nothing else re-arms it.
func TestAChangedValueWakesAWatcher(t *testing.T) {
	e, a, _, aID, bID := linked(t, Succeeded(), Succeeded())
	a.commits("v1")
	settled(t, e)

	a.commits("v2")
	e.Wake(subj, aID)
	runNext(t, e)

	assert.Equal(t, keyOf(e, bID), takeRun(t, e))
}

// The supervisor never compares values — the body says whether its answer moved by committing or
// not. Without this the wake fires every cycle and the interval stops meaning anything.
func TestACommitThatCarriedNoValueWakesNobody(t *testing.T) {
	e, a, _, aID, _ := linked(t, Succeeded(), Succeeded())
	a.commits("v1")
	settled(t, e)

	a.commitsNothing()
	e.Wake(subj, aID)
	runNext(t, e)

	noRuns(t, e, "an unchanged answer woke a dependent")
}

// A wake goes through the run queue, so it overrides a suspension exactly as Wake does — which
// is what re-asks a reconciler parked "for the life of the connection" when a new one arrives.
func TestAChangedValueWakesASuspendedWatcher(t *testing.T) {
	e, a, _, aID, bID := linked(t, Succeeded(), Suspend("Unsupported", "no such endpoint"))
	a.commits("v1")
	settled(t, e)
	require.False(t, att(t, e, bID).Scheduled(), "the dependent is suspended")

	a.commits("v2")
	e.Wake(subj, aID)
	runNext(t, e)

	assert.Equal(t, keyOf(e, bID), takeRun(t, e))
}

// The data edge does not consult health: a reconciler declaring only a dependency must not be gated
// by health it never declared, so a failing run that learned something still wakes it.
func TestAFailedRunThatChangedItsValueStillWakes(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	a := &steered{res: Succeeded()}
	watcher := &steered{res: Succeeded()}
	Register(e, "conn", a)
	Register(e, "watcher", watcher, WithWatches("conn"))
	a.commits("v1")
	settled(t, e)

	a.set(Fail("Unreachable", assert.AnError))
	a.commits("v2")
	e.Wake(subj, "conn")
	runNext(t, e)

	assert.Equal(t, keyOf(e, "watcher"), takeRun(t, e))
}

// A wake is not a way past a dependency. The dependent here is already suspended for the
// outage, so only the data edge can reach it — and what it earns is a record, never a dial: the
// edge costs a run, never a timeout, and "one timeout per cycle" survives it.
func TestAWokenRunWithAFailingDependencyRecordsRatherThanDialing(t *testing.T) {
	e, a, b, aID, bID := linked(t, Succeeded(), Succeeded())
	a.commits("v1")
	settled(t, e)
	a.set(Fail("Unreachable", assert.AnError))
	a.commitsNothing()
	e.Wake(subj, aID)
	runNext(t, e)                                  // the connection fails
	require.Equal(t, keyOf(e, bID), runNext(t, e)) // the dependent suspends on it
	require.Equal(t, ReasonDependencyFailed, att(t, e, bID).LastAttempt.Reason)
	noRuns(t, e, "the dependent is suspended for the rest of the outage")
	before := b.count()

	a.commits("v2") // still failing, but it learned something
	e.Wake(subj, aID)
	runNext(t, e)
	require.Equal(t, keyOf(e, bID), runNext(t, e), "the data edge reached it")

	assert.Equal(t, before, b.count(), "the body dialed a server the state already said was down")
	assert.Equal(t, ReasonDependencyFailed, att(t, e, bID).LastAttempt.Reason)
}

// Watches take IDs Register already returned, like dependencies, so a cascade only ever runs
// forward and the wiring bug is caught at registration.
func TestRegisterPanicsOnAWatchNotYetRegistered(t *testing.T) {
	e := New()

	assert.Panics(t, func() { Register(e, "uid", &steered{}, WithWatches("nope")) })
}

// --- what a run records ---

// The last value a run commits is the one that lands, and it lands once: one attempt, and one
// wake for whoever watches it.
func TestARunCommitsTheLastValueItRecorded(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	Register(e, "conn", reconcilerFunc(func(_ context.Context, pass *Pass[string]) Result {
		pass.Commit("v1")
		pass.Commit("v2")
		return Succeeded()
	}))
	Register(e, "uid", &steered{res: Succeeded()}, WithWatches("conn"))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	read, _ := e.Read(subj)
	assert.Equal(t, "v2", Get[string](read, "conn").Value)
	assert.Equal(t, keyOf(e, "uid"), takeRun(t, e))
	noRuns(t, e, "the watcher was woken more than once")
}

// A body may record what it found before it works out how to classify it, so a run that ends in
// Skip discards the value with the record — Skip records nothing, and that has to mean nothing.
func TestARunThatSkipsCommitsNothingItRecorded(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	Register(e, "conn", reconcilerFunc(func(_ context.Context, pass *Pass[string]) Result {
		pass.Commit("v1")
		return Skip()
	}))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	read, _ := e.Read(subj)
	o := Get[string](read, "conn")
	assert.False(t, o.Known(), "a skipped run published a value")
	assert.Empty(t, o.Value)
}

// Containment is all or nothing: a panicking body's buffered value goes with the wreckage, or a
// half-finished run publishes an answer the record beside it calls Internal.
func TestAPanickingRunCommitsNothingItRecorded(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	Register(e, "conn", reconcilerFunc(func(_ context.Context, pass *Pass[string]) Result {
		pass.Commit("v1")
		panic("boom")
	}))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	read, _ := e.Read(subj)
	o := Get[string](read, "conn")
	assert.Empty(t, o.Value, "a panicking run published a value")
	assert.False(t, o.Known())
	assert.Equal(t, ReasonInternal, o.LastAttempt.Reason)
}

// --- failure containment ---

// A panicking body must still produce a record and free its key — otherwise the reconciler reads as
// in flight forever and the key stays held in the queue.
func TestAPanickingRunRecordsInternalAndFreesItsKey(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	Register(e, "conn", reconcilerFunc(func(context.Context, *Pass[string]) Result {
		panic("boom")
	}))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, "conn")
	assert.Equal(t, VerdictFailed, a.LastAttempt.Verdict)
	assert.Equal(t, ReasonInternal, a.LastAttempt.Reason)
	assert.False(t, a.InFlight())

	e.Wake(subj, "conn")
	assert.Equal(t, keyOf(e, "conn"), takeRun(t, e), "the key stayed held")
}

// A body that hands back nothing is the same class of bug as one that panics, and is contained
// the same way. Panicking over it would take the supervisor's lock down with the worker.
func TestARunThatReturnsNoResultIsRecordedAsInternal(t *testing.T) {
	e, _, id := single(t, Result{})
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, id)
	assert.Equal(t, VerdictFailed, a.LastAttempt.Verdict)
	assert.Equal(t, ReasonInternal, a.LastAttempt.Reason)
	assert.False(t, a.InFlight())
}

// The supervisor deadlines the context it hands the run; the body classifies the expiry itself.
func TestARunIsBoundedByItsTimeout(t *testing.T) {
	e, p, _ := single(t, Succeeded(), WithTimeout(50*time.Millisecond))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	p.mu.Lock()
	defer p.mu.Unlock()
	assert.True(t, p.sawDeadline)
}

// --- publishing ---

// The supervisor reports every pass; the caller decides what is news.
func TestOnPassFiresAfterEveryPassInOrderPerSubject(t *testing.T) {
	e, _, id := single(t, Succeeded())
	var calls []Snapshot
	e.OnPass(func(name string, snap Snapshot) {
		require.Equal(t, subj, name)
		calls = append(calls, snap)
	})
	e.Add(subj)
	e.settle()
	require.Len(t, calls, 1, "the fresh subject's own pass")
	assert.False(t, calls[0].Attempts(id).LastAttempt.Done())

	runNext(t, e)

	require.Len(t, calls, 2)
	assert.True(t, calls[1].Attempts(id).OK(), "the second call carries the committed run")
}

// LastSeen dates the value, not the run: a failure that follows does not un-read what was read,
// so the value and its provenance both stand.
func TestAFailureLeavesTheValueAndItsLastSeenStanding(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	uid := "uid-1"
	p := &steered{res: Succeeded(), next: &uid}
	Register(e, "conn", p)
	e.Add(subj)
	e.settle()
	runNext(t, e)
	read, _ := e.Read(subj)
	seen := Get[string](read, "conn")
	require.True(t, seen.Known())

	p.set(Fail("Unreachable", assert.AnError))
	p.commitsNothing()
	e.Wake(subj, "conn")
	runNext(t, e)

	read, _ = e.Read(subj)
	after := Get[string](read, "conn")
	assert.Equal(t, "uid-1", after.Value, "the value outlives the failure")
	assert.Equal(t, seen.LastSeen, after.LastSeen, "a failure that read nothing is not a read")
	assert.False(t, after.OK())
}

// A success dates the value it confirmed, so a run that has never had a value to confirm dates
// nothing — otherwise Known() passes and hands the reader a zero it will dereference.
func TestASuccessWithNoValueEverCommittedIsNotKnown(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	Register(e, "conn", &steered{res: Succeeded()}) // succeeds, commits nothing
	e.Add(subj)
	e.settle()

	runNext(t, e)

	read, _ := e.Read(subj)
	o := Get[string](read, "conn")
	require.True(t, o.OK(), "the run did succeed")
	assert.False(t, o.Known(), "there is no value to read")
	assert.True(t, o.LastSeen.IsZero())
}

// Every success dates the value, whether or not it committed a new one: a run that found the
// answer unchanged still read it.
func TestASuccessWithNoNewValueStillAdvancesLastSeen(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	uid := "uid-1"
	p := &steered{res: Succeeded(), next: &uid}
	Register(e, "conn", p)
	e.Add(subj)
	e.settle()
	runNext(t, e)
	read, _ := e.Read(subj)
	first := Get[string](read, "conn")

	p.mu.Lock()
	p.next = nil
	p.mu.Unlock()
	e.Wake(subj, "conn")
	runNext(t, e)

	read, _ = e.Read(subj)
	after := Get[string](read, "conn")
	assert.Equal(t, "uid-1", after.Value, "nothing new to commit, so the value stands")
	assert.Equal(t, after.LastAttempt.FinishedAt, after.LastSeen, "the run that confirmed it")
	assert.True(t, after.LastSeen.After(first.LastSeen))
}

func TestReadReportsAnUntrackedSubject(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	uid := "uid-1"
	Register(e, "conn", &steered{res: Succeeded(), next: &uid})

	_, ok := e.Read(subj)
	require.False(t, ok)

	e.Add(subj)
	e.settle()
	runNext(t, e)

	v, ok := e.Read(subj)
	require.True(t, ok)
	o := Get[string](v, "conn")
	assert.Equal(t, "uid-1", o.Value, "the run's committed value")
	assert.True(t, o.OK())
	assert.True(t, o.Known(), "the supervisor stamps when the value was read")
}

// A run that hands back no value keeps the previous one: an Observation outlives the failure
// that follows it, and the failing run is still recorded beside it.
func TestANilValueKeepsThePreviousOne(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	uid := "uid-1"
	p := &steered{res: Succeeded(), next: &uid}
	Register(e, "conn", p)
	e.Add(subj)
	e.settle()
	runNext(t, e)

	p.set(Fail("Unreachable", assert.AnError))
	p.next = nil
	e.Wake(subj, "conn")
	runNext(t, e)

	v, ok := e.Read(subj)
	require.True(t, ok)
	o := Get[string](v, "conn")
	assert.Equal(t, "uid-1", o.Value, "the failure erased the last answer")
	assert.True(t, o.Known())
	assert.False(t, o.OK())
}

func TestCloseStopsTheTimersAndTheQueues(t *testing.T) {
	e, _, _ := single(t, Fail("Unreachable", assert.AnError))
	e.Add(subj)
	e.settle()
	runNext(t, e)
	sub := e.subjects[subj]
	require.NotNil(t, sub.timer)

	require.NoError(t, e.Close())

	assert.Nil(t, sub.timer)
	assert.Empty(t, e.subjects)
	_, ok := e.runQ.Next(within(t))
	assert.False(t, ok, "the run queue outlived Close")
}

// --- values the supervisor drops ---

// discarding is a reconciler whose committed values it records when the supervisor hands them back.
type discarding struct {
	steered
	discarded *testutil.Probe[string]
	// panicAfterCommit is a body that commits and then panics, which steered cannot express:
	// its hook runs before the commit.
	panicAfterCommit string
}

func (p *discarding) Discard(v string) { p.discarded.Fire(v) }

func (p *discarding) Reconcile(ctx context.Context, pass *Pass[string]) Result {
	if p.panicAfterCommit != "" {
		pass.Commit(p.panicAfterCommit)
		panic("reconciler body bug")
	}
	return p.steered.Reconcile(ctx, pass)
}

// A committed value can own something — a connection, a file — so a value the supervisor drops is
// handed back rather than dropped silently: nothing else can reach it to release it.
func TestARefusedCommitIsHandedBack(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	p := &discarding{discarded: testutil.NewProbe[string](4)}
	p.res = Succeeded()
	p.commits("built")
	// Removing the subject from inside the run is the race a Release loses: the commit
	// arrives against a subject that has gone.
	p.onRun = func() { e.Remove("prod") }
	Register(e, "conn", p)
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")

	assert.Equal(t, "built", p.discarded.Await(t, "the refused value"))
}

// A Skip records nothing, so a value the body committed before concluding one goes the same way.
func TestASkippedRunsValueIsHandedBack(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	p := &discarding{discarded: testutil.NewProbe[string](4)}
	p.res = Skip()
	p.commits("built")
	Register(e, "conn", p)
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")

	assert.Equal(t, "built", p.discarded.Await(t, "the skipped value"))
}

// A body that panics has still committed whatever it committed before it did, and the supervisor
// applies none of it — so that value is handed back too, or a panic leaks what it built.
func TestAPanickingRunsValueIsHandedBack(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	p := &discarding{discarded: testutil.NewProbe[string](4), panicAfterCommit: "built"}
	p.res = Succeeded()
	Register(e, "conn", p)
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")

	assert.Equal(t, "built", p.discarded.Await(t, "the panicking run's value"))
}

// The ordinary path: a value the supervisor applied is the reconciler's own to read back, and handing it
// back as dropped would have a caller release what is live.
func TestAnAppliedValueIsNotHandedBack(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	p := &discarding{discarded: testutil.NewProbe[string](4)}
	p.res = Succeeded()
	p.commits("built")
	Register(e, "conn", p)
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")

	require.Eventually(t, func() bool {
		snap, ok := e.Read("prod")
		return ok && Get[string](snap, "conn").Value == "built"
	}, testutil.Timeout, time.Millisecond, "the value to land")
	_, dropped := p.discarded.TryAwait()
	assert.False(t, dropped, "an applied value is not a dropped one")
}

// The supervisor is what tells a body a value has landed, so a reconciler committing the zero T lands it
// once and reads as known from then on — without which such a reconciler reports itself never
// observed for as long as its answer stays the zero value.
func TestTheZeroValueLandsAndReadsAsKnown(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	seen := testutil.NewProbe[bool](4)
	Register(e, "conn", reconcilerFunc(func(_ context.Context, pass *Pass[string]) Result {
		seen.Fire(pass.Known())
		if !pass.Known() {
			pass.Commit("")
		}
		return Succeeded()
	}), WithInterval(time.Millisecond))
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")

	assert.False(t, seen.Await(t, "the first run"), "nothing has landed yet")
	assert.True(t, seen.Await(t, "the run after it"), "the zero value it committed")
	require.Eventually(t, func() bool {
		snap, ok := e.Read("prod")
		return ok && Get[string](snap, "conn").Known()
	}, testutil.Timeout, time.Millisecond, "the observation to date itself")
}

// A run that failed can still have read something — which components are down — so the value it
// commits is dated now. Dating it by the last success instead leaves a value nothing has ever
// confirmed, and a reconciler that has only ever failed reads as never observed while holding the
// answer a caller came for.
func TestAFailedRunsCommittedValueIsDated(t *testing.T) {
	e, p, name := single(t, Fail("Unreachable", assert.AnError))
	p.commits("etcd")
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")

	require.Eventually(t, func() bool {
		snap, ok := e.Read("prod")
		return ok && Get[string](snap, name).Known()
	}, testutil.Timeout, time.Millisecond, "the failing answer to be dated")
	snap, _ := e.Read("prod")
	assert.Equal(t, "etcd", Get[string](snap, name).Value)
	assert.False(t, Get[string](snap, name).OK(), "dated, and still a failure")
}

// A success that commits nothing re-confirms what stands, which is what makes "identified, as of
// 10:00" readable — but a success with nothing to date must leave seen alone, or a reader's Known
// guard passes and hands it the zero value.
func TestASuccessWithNothingToDateLeavesTheObservationUnknown(t *testing.T) {
	e, _, name := single(t, Succeeded())
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")

	require.Eventually(t, func() bool {
		snap, ok := e.Read("prod")
		return ok && Get[string](snap, name).OK()
	}, testutil.Timeout, time.Millisecond, "the run to land")
	snap, _ := e.Read("prod")
	assert.False(t, Get[string](snap, name).Known(), "nothing was ever read")
}

// The supervisor hands back every value it stops holding. A value owning a connection was fine
// to drop — the pool retires it — but one owning a goroutine leaks unless the reconciler is told,
// so dropping the subject is a hand-back like a refused commit is.
func TestRemoveHandsBackTheStandingValue(t *testing.T) {
	e, p := standing(t, "built")

	e.Remove("prod")

	assert.Equal(t, "built", p.discarded.Await(t, "the standing value"))
}

func TestCloseHandsBackEveryStandingValue(t *testing.T) {
	e, p := standing(t, "built")

	require.NoError(t, e.Close())

	assert.Equal(t, "built", p.discarded.Await(t, "the standing value"))
}

// A subject with nothing committed has nothing to hand back, and a second Remove has nothing
// left: a handle is handed back exactly once, or a caller would join a goroutine twice.
func TestAValueIsHandedBackOnceAndOnlyIfThereIsOne(t *testing.T) {
	e, p := standing(t, "built")

	e.Remove("prod")
	assert.Equal(t, "built", p.discarded.Await(t, "the standing value"))

	e.Remove("prod")
	e.Add("empty")
	e.Remove("empty")
	require.NoError(t, e.Close())

	// Deterministic rather than a window: Remove and Close discard inline, so anything they
	// hand back has fired by the time they return.
	_, more := p.discarded.TryAwait()
	assert.False(t, more, "nothing more to hand back")
}

// standing is a supervisor holding one committed value, ready for whatever drops it.
func standing(t *testing.T, value string) (*Supervisor, *discarding) {
	t.Helper()
	e := New()
	p := &discarding{discarded: testutil.NewProbe[string](4)}
	p.res = Succeeded()
	p.commits(value)
	Register(e, "conn", p)
	stop := e.Start(t.Context())
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })

	e.Add("prod")
	require.Eventually(t, func() bool {
		snap, ok := e.Read("prod")
		return ok && Get[string](snap, "conn").Known()
	}, testutil.Timeout, time.Millisecond, "the value to stand")
	return e, p
}

// A commit REPLACES the standing value and does not hand it back — the one place the supervisor
// drops a value without telling the reconciler. A commit often carries the last value's holdings
// forward: kubeconn's connInfo is a struct whose runs commit a copy with one field moved and the
// same live connection inside, and handing that back would retire a connection still in use.
// What a run stops holding is the run's to release before it commits.
func TestAReplacedValueIsNotHandedBack(t *testing.T) {
	e, p := standing(t, "first")

	p.commits("second")
	e.Wake("prod", "conn")

	require.Eventually(t, func() bool {
		snap, ok := e.Read("prod")
		return ok && Get[string](snap, "conn").Value == "second"
	}, testutil.Timeout, time.Millisecond, "the committed value to stand")

	_, handedBack := p.discarded.TryAwait()
	assert.False(t, handedBack, "a replaced value stays the reconciler's to reason about")
}
