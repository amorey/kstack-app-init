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

package probe

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

// steered is a probe the test steers: the result is swapped at will, runs are counted, and an
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

func (p *steered) Run(ctx context.Context, _ string, _ string, _ Snapshot) (Result, *string) {
	p.mu.Lock()
	p.runs++
	_, p.sawDeadline = ctx.Deadline()
	res, next, hook := p.res, p.next, p.onRun
	p.mu.Unlock()

	if hook != nil {
		hook()
	}
	return res, next
}

func (p *steered) set(res Result) { p.mu.Lock(); defer p.mu.Unlock(); p.res = res }
func (p *steered) count() int     { p.mu.Lock(); defer p.mu.Unlock(); return p.runs }

// probeFunc adapts a bare func for a test that needs a one-off body.
type probeFunc func(ctx context.Context, subject string, prev string, snap Snapshot) (Result, *string)

func (f probeFunc) Run(ctx context.Context, subject string, prev string, snap Snapshot) (Result, *string) {
	return f(ctx, subject, prev, snap)
}

// single is an engine with one steered probe, closed on cleanup.
func single(t *testing.T, res Result, opts ...ProbeOption) (*Engine, *steered, ID) {
	t.Helper()
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	p := &steered{res: res}
	return e, p, Register(e, "conn", p, opts...)
}

// pair is single plus a probe requiring the first.
func pair(t *testing.T, aRes, bRes Result) (e *Engine, a, b *steered, aID, bID ID) {
	t.Helper()
	e = New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	a, b = &steered{res: aRes}, &steered{res: bRes}
	aID = Register(e, "conn", a)
	bID = Register(e, "uid", b, WithDependencies(aID))
	return e, a, b, aID, bID
}

// linked is a pair where the second declares both edges on the first — the shape kubeconn's
// watchers have, and the only one where a value change and a health gate are both in play.
func linked(t *testing.T, aRes, bRes Result) (e *Engine, a, b *steered, aID, bID ID) {
	t.Helper()
	e = New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	a, b = &steered{res: aRes}, &steered{res: bRes}
	aID = Register(e, "conn", a)
	bID = Register(e, "uid", b, WithDependencies(aID), WithWatches(aID))
	return e, a, b, aID, bID
}

// commits makes the body hand back v, which the engine reads as "the value moved".
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

// settled runs both probes of a linked pair to a succeeded rest, so what follows is measured
// against two probes parked on their intervals rather than against a fresh subject's first pass.
func settled(t *testing.T, e *Engine) {
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
func (e *Engine) settle() {
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
func runNext(t *testing.T, e *Engine) key {
	t.Helper()
	k, ok := e.runQ.Next(within(t))
	require.True(t, ok, "the run queue closed")
	e.runProbe(t.Context(), k)
	e.runQ.Done(k)
	e.settle()
	return k
}

// takeRun pops one due run without executing it, giving the key back.
func takeRun(t *testing.T, e *Engine) key {
	t.Helper()
	k, ok := e.runQ.Next(within(t))
	require.True(t, ok, "the run queue closed")
	e.runQ.Done(k)
	return k
}

// noRuns is a negative assertion, so it needs a bounded window rather than the failsafe: it
// fails the instant a run is handed out.
func noRuns(t *testing.T, e *Engine, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if k, ok := e.runQ.Next(ctx); ok {
		e.runQ.Done(k)
		t.Fatalf("%s: %v was handed out", msg, k)
	}
}

// att reads one probe's attempts through Read.
func att(t *testing.T, e *Engine, id ID) Attempts {
	t.Helper()
	v, ok := e.Read(subj)
	require.True(t, ok, "subject not tracked")
	return v.Attempts(id)
}

func startEngine(t *testing.T, e *Engine) {
	t.Helper()
	stop, err := e.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, stop(context.Background())) })
}

// --- registration ---

func TestRegisterReturnsIDsInRegistrationOrder(t *testing.T) {
	e, _, a := single(t, Skip())
	b := Register(e, "uid", &steered{}, WithDependencies(a))

	assert.Equal(t, ID(0), a)
	assert.Equal(t, ID(1), b)
	assert.Equal(t, "uid", e.specs[b].name)
	assert.Equal(t, []ID{a}, e.specs[b].cfg.dependencies)
}

func TestRegisterPanicsOnADuplicateName(t *testing.T) {
	e, p, _ := single(t, Skip())

	assert.Panics(t, func() { Register(e, "conn", p) })
}

// WithDependencies takes IDs Register already returned, so the graph is acyclic by construction;
// this is the backstop for a hand-forged ID.
func TestRegisterPanicsOnADependencyNotYetRegistered(t *testing.T) {
	e := New()

	assert.Panics(t, func() { Register(e, "uid", &steered{}, WithDependencies(ID(0))) })
}

// A registration states only what deviates from the package defaults.
func TestRegistrationDefaultsWhatItDoesNotState(t *testing.T) {
	e, _, id := single(t, Skip(), WithInterval(10*time.Minute))

	cfg := e.specs[id].cfg
	assert.Equal(t, 10*time.Minute, cfg.interval)
	assert.Equal(t, defaultCfg.timeout, cfg.timeout)
	assert.Equal(t, defaultCfg.backoff, cfg.backoff)
}

func TestRegisterPanicsAfterStart(t *testing.T) {
	e, p, _ := single(t, Skip())
	startEngine(t, e)

	assert.Panics(t, func() { Register(e, "late", p) })
}

// A subject's bookkeeping is sized when it is added, so the probe set must be complete first.
func TestRegisterPanicsAfterAdd(t *testing.T) {
	e, p, _ := single(t, Skip())
	e.Add(subj)

	assert.Panics(t, func() { Register(e, "late", p) })
}

// --- subjects and the pass ---

// A fresh subject owes exactly its runnable probes: the dependent waits on an answer, and a
// second Add of the same name asks for nothing.
func TestAddQueuesWhatAFreshSubjectOwes(t *testing.T) {
	e, _, _, aID, _ := pair(t, Skip(), Skip())

	e.Add(subj)
	e.settle()

	assert.Equal(t, key{subject: subj, probe: aID}, takeRun(t, e))
	noRuns(t, e, "more than the connection probe was queued")

	e.Add(subj)
	e.settle()
	noRuns(t, e, "a second Add asked for a run")
}

func TestWorkQueuedBeforeStartIsServedOnceItRuns(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	ran := testutil.NewProbe[struct{}](1)
	Register(e, "conn", probeFunc(func(context.Context, string, string, Snapshot) (Result, *string) {
		ran.Fire(struct{}{})
		return Suspend("Resolved", ""), nil
	}))
	e.Add(subj)

	startEngine(t, e)

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

	assert.Equal(t, key{subject: subj, probe: id}, takeRun(t, e), "the mid-run Wake was folded into the run")
}

// A Wake overrides suspension — it is the one input the derivation cannot produce.
func TestAWakeRunsASuspendedProbe(t *testing.T) {
	e, _, id := single(t, Suspend("ContextNotFound", ""))
	e.Add(subj)
	e.settle()
	runNext(t, e)
	noRuns(t, e, "a suspended probe was scheduled")

	e.Wake(subj, id)

	assert.Equal(t, key{subject: subj, probe: id}, takeRun(t, e))
}

func TestWakeAllReachesEveryTrackedSubject(t *testing.T) {
	e, _, id := single(t, Suspend("ContextNotFound", ""))
	e.Add("ctx-1")
	e.Add("ctx-2")
	e.settle()
	seen := []key{runNext(t, e), runNext(t, e)} // the fresh subjects' own runs
	require.ElementsMatch(t, seen, []key{{"ctx-1", id}, {"ctx-2", id}})

	e.WakeAll(id)

	assert.ElementsMatch(t,
		[]key{takeRun(t, e), takeRun(t, e)},
		[]key{{"ctx-1", id}, {"ctx-2", id}})
}

// --- the schedule ---

// NextAttempt is the run in flight; writing a schedule over it would erase both the in-flight
// mark and the schedule it was dispatched on. Its commit passes again.
func TestARunInFlightIsLeftAlone(t *testing.T) {
	e, _, id := single(t, Skip())
	e.Add(subj)
	e.settle()
	require.Equal(t, key{subject: subj, probe: id}, takeRun(t, e), "the fresh subject's own run")
	// The worker that took it marks the run before dropping the lock. Safe unlocked here: no
	// loops are running.
	e.subjects[subj].obs[id].begin(runAt)

	e.pass(subj)

	a := att(t, e, id)
	assert.Equal(t, runAt, a.NextAttempt.StartedAt, "the pass wrote over the run in flight")
	noRuns(t, e, "a second run of a probe already out")
}

// The success-path poll is the default; Suspend is the only opt-out.
func TestASucceededProbeIsDueAgainAfterItsInterval(t *testing.T) {
	e, _, id := single(t, Succeeded(), WithInterval(250*time.Millisecond))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, id)
	require.True(t, a.OK())
	assert.Equal(t, a.LastAttempt.FinishedAt.Add(250*time.Millisecond), a.NextAttempt.ScheduledAt)
}

// The ladder widens per consecutive failure and holds at the cap — and it is a pure function of
// Failures, so a later pass derives the same ScheduledAt it derived before.
func TestAFailedProbeClimbsTheLadder(t *testing.T) {
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
// brings the probe back, so an unreadable source does not become a busy loop.
func TestASkipLeavesNoRecordAndNothingScheduled(t *testing.T) {
	e, p, id := single(t, Fail("ResolveFailed", assert.AnError))
	e.Add(subj)
	e.settle()
	runNext(t, e)
	failed := att(t, e, id).LastAttempt
	require.True(t, failed.Done())

	p.set(Skip())
	e.Wake(subj, id)
	runNext(t, e)

	a := att(t, e, id)
	assert.Equal(t, failed, a.LastAttempt, "a Skip must leave the last record standing")
	assert.False(t, a.Scheduled())
	noRuns(t, e, "a skipped probe was re-dispatched")
}

// The timer only brings the pass back; the pass decides again per probe.
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

	require.Equal(t, key{subject: subj, probe: aID}, takeRun(t, e))
	b := att(t, e, bID)
	assert.False(t, b.LastAttempt.Done())
	assert.False(t, b.Scheduled())
}

// One run records DependencyFailed — never dialed, so no StartedAt — and the rest of the outage
// costs nothing: one timeout per cycle, not one per probe.
func TestADependentRecordsDependencyFailedOnceThenSuspends(t *testing.T) {
	e, _, bBody, aID, bID := pair(t, Fail("Unreachable", assert.AnError), Succeeded())
	e.Add(subj)
	e.settle()
	require.Equal(t, key{subject: subj, probe: aID}, runNext(t, e), "the connection answers first")

	require.Equal(t, key{subject: subj, probe: bID}, runNext(t, e), "the failure makes the dependent due once")

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

	assert.Equal(t, key{subject: subj, probe: bID}, takeRun(t, e))
}

// A dependency that failed between the pass and the worker picking the key up means the run is
// recorded, not dialed — a worker must not spend a timeout learning what the state already says.
func TestDependenciesAreRecheckedAtDispatch(t *testing.T) {
	e, aBody, bBody, aID, bID := pair(t, Succeeded(), Succeeded())
	e.Add(subj)
	e.settle()
	runNext(t, e) // connection succeeds; the dependent is now queued

	aBody.set(Fail("Unreachable", assert.AnError))
	e.runProbe(t.Context(), key{subject: subj, probe: aID}) // the connection dies before b dispatches
	e.settle()
	require.Equal(t, key{subject: subj, probe: bID}, runNext(t, e))

	assert.Zero(t, bBody.count(), "the body dialed a server the state already said was down")
	assert.Equal(t, ReasonDependencyFailed, att(t, e, bID).LastAttempt.Reason)
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

	assert.Equal(t, key{subject: subj, probe: bID}, takeRun(t, e))
}

// The engine never compares values — the body says whether its answer moved by committing or
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
// is what re-asks a probe parked "for the life of the connection" when a new one arrives.
func TestAChangedValueWakesASuspendedWatcher(t *testing.T) {
	e, a, _, aID, bID := linked(t, Succeeded(), Suspend("Unsupported", "no such endpoint"))
	a.commits("v1")
	settled(t, e)
	require.False(t, att(t, e, bID).Scheduled(), "the dependent is suspended")

	a.commits("v2")
	e.Wake(subj, aID)
	runNext(t, e)

	assert.Equal(t, key{subject: subj, probe: bID}, takeRun(t, e))
}

// The data edge does not consult health: a probe declaring only a dependency must not be gated
// by health it never declared, so a failing run that learned something still wakes it.
func TestAFailedRunThatChangedItsValueStillWakes(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	a := &steered{res: Succeeded()}
	watcher := &steered{res: Succeeded()}
	aID := Register(e, "conn", a)
	wID := Register(e, "watcher", watcher, WithWatches(aID))
	a.commits("v1")
	settled(t, e)

	a.set(Fail("Unreachable", assert.AnError))
	a.commits("v2")
	e.Wake(subj, aID)
	runNext(t, e)

	assert.Equal(t, key{subject: subj, probe: wID}, takeRun(t, e))
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
	runNext(t, e)                                                   // the connection fails
	require.Equal(t, key{subject: subj, probe: bID}, runNext(t, e)) // the dependent suspends on it
	require.Equal(t, ReasonDependencyFailed, att(t, e, bID).LastAttempt.Reason)
	noRuns(t, e, "the dependent is suspended for the rest of the outage")
	before := b.count()

	a.commits("v2") // still failing, but it learned something
	e.Wake(subj, aID)
	runNext(t, e)
	require.Equal(t, key{subject: subj, probe: bID}, runNext(t, e), "the data edge reached it")

	assert.Equal(t, before, b.count(), "the body dialed a server the state already said was down")
	assert.Equal(t, ReasonDependencyFailed, att(t, e, bID).LastAttempt.Reason)
}

// Watches take IDs Register already returned, like dependencies, so a cascade only ever runs
// forward and the wiring bug is caught at registration.
func TestRegisterPanicsOnAWatchNotYetRegistered(t *testing.T) {
	e := New()

	assert.Panics(t, func() { Register(e, "uid", &steered{}, WithWatches(ID(0))) })
}

// --- failure containment ---

// A panicking body must still produce a record and free its key — otherwise the probe reads as
// in flight forever and the key stays held in the queue.
func TestAPanickingRunRecordsInternalAndFreesItsKey(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	id := Register(e, "conn", probeFunc(func(context.Context, string, string, Snapshot) (Result, *string) {
		panic("boom")
	}))
	e.Add(subj)
	e.settle()

	runNext(t, e)

	a := att(t, e, id)
	assert.Equal(t, VerdictFailed, a.LastAttempt.Verdict)
	assert.Equal(t, ReasonInternal, a.LastAttempt.Reason)
	assert.False(t, a.InFlight())

	e.Wake(subj, id)
	assert.Equal(t, key{subject: subj, probe: id}, takeRun(t, e), "the key stayed held")
}

// A body that hands back nothing is the same class of bug as one that panics, and is contained
// the same way. Panicking over it would take the engine's lock down with the worker.
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

// The engine deadlines the context it hands the run; the body classifies the expiry itself.
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

// The engine reports every pass; the caller decides what is news.
func TestOnChangeFiresAfterEveryPassInOrderPerSubject(t *testing.T) {
	e, _, id := single(t, Succeeded())
	var calls []Snapshot
	e.OnChange(func(name string, snap Snapshot) {
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
	id := Register(e, "conn", p)
	e.Add(subj)
	e.settle()
	runNext(t, e)
	read, _ := e.Read(subj)
	seen := Get[string](read, "conn")
	require.True(t, seen.Known())

	p.set(Fail("Unreachable", assert.AnError))
	e.Wake(subj, id)
	runNext(t, e)

	read, _ = e.Read(subj)
	after := Get[string](read, "conn")
	assert.Equal(t, "uid-1", after.Value, "the value outlives the failure")
	assert.Equal(t, seen.LastSeen, after.LastSeen, "a failure is not a read")
	assert.False(t, after.OK())
}

// --- reading an observable ---

// A body reads a sibling by the name it was registered under, with no wiring: the whole
// observation comes back, value beside the attempts that account for it.
func TestGetReadsAnObservableByName(t *testing.T) {
	e, p, _ := single(t, Succeeded())
	p.commits("v1")
	e.Add(subj)
	e.settle()
	runNext(t, e)

	read, _ := e.Read(subj)
	o := Get[string](read, "conn")

	assert.Equal(t, "v1", o.Value)
	assert.True(t, o.Known())
	assert.True(t, o.OK(), "the attempts come with it")
}

// A name nothing was registered under is a wiring bug, loud like the rest of them.
func TestGetPanicsOnAnUnregisteredName(t *testing.T) {
	e, _, _ := single(t, Succeeded())
	e.Add(subj)
	read, _ := e.Read(subj)

	assert.Panics(t, func() { Get[string](read, "nope") })
}

// The name and the type are stated separately, so they can disagree. A read that asks for the
// wrong one is a wiring bug too — never a zero value passed off as an answer.
func TestGetPanicsOnTheWrongType(t *testing.T) {
	e, p, _ := single(t, Succeeded())
	p.commits("v1")
	e.Add(subj)
	e.settle()
	runNext(t, e)
	read, _ := e.Read(subj)

	assert.Panics(t, func() { Get[int](read, "conn") })
}

// Before anything is committed there is no value to type-check against, so the read answers
// "nothing known yet" rather than guessing — and the attempts still explain why.
func TestGetOnAProbeThatHasNotCommittedIsNotKnown(t *testing.T) {
	e, _, _ := single(t, Fail("Unreachable", assert.AnError))
	e.Add(subj)
	e.settle()
	runNext(t, e)

	read, _ := e.Read(subj)
	o := Get[string](read, "conn")

	assert.False(t, o.Known())
	assert.Equal(t, Reason("Unreachable"), o.LastAttempt.Reason)
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
	id := Register(e, "conn", p)
	e.Add(subj)
	e.settle()
	runNext(t, e)
	read, _ := e.Read(subj)
	first := Get[string](read, "conn")

	p.mu.Lock()
	p.next = nil
	p.mu.Unlock()
	e.Wake(subj, id)
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
	assert.True(t, o.Known(), "the engine stamps when the value was read")
}

// A run that hands back no value keeps the previous one: an Observation outlives the failure
// that follows it, and the failing run is still recorded beside it.
func TestANilValueKeepsThePreviousOne(t *testing.T) {
	e := New()
	t.Cleanup(func() { assert.NoError(t, e.Close()) })
	uid := "uid-1"
	p := &steered{res: Succeeded(), next: &uid}
	id := Register(e, "conn", p)
	e.Add(subj)
	e.settle()
	runNext(t, e)

	p.set(Fail("Unreachable", assert.AnError))
	p.next = nil
	e.Wake(subj, id)
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
