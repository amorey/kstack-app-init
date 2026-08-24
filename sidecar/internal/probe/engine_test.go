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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noRun is a probe body for tests that never dispatch it.
func noRun(context.Context, struct{}) (Result, func(*struct{})) { return Skip(), nil }

// --- registration (implemented) ---

func TestRegisterReturnsIDsInRegistrationOrder(t *testing.T) {
	e := New[struct{}]()

	a := e.Register("a", noRun)
	b := e.Register("b", noRun, Needs(a))

	assert.Equal(t, ID(0), a)
	assert.Equal(t, ID(1), b)
	require.Len(t, e.specs, 2)
	assert.Equal(t, []ID{a}, e.specs[b].cfg.needs)
}

func TestRegisterPanicsOnADuplicateName(t *testing.T) {
	e := New[struct{}]()
	e.Register("a", noRun)

	assert.Panics(t, func() { e.Register("a", noRun) })
}

// Needs takes IDs Register already returned, so the graph is acyclic by construction; this is
// the backstop for a hand-forged ID.
func TestRegisterPanicsOnANeedNotYetRegistered(t *testing.T) {
	e := New[struct{}]()

	assert.Panics(t, func() { e.Register("a", noRun, Needs(ID(0))) })
}

// A registration states only what deviates from the package defaults.
func TestRegistrationDefaultsWhatItDoesNotState(t *testing.T) {
	e := New[struct{}]()

	id := e.Register("a", noRun, WithInterval(10*time.Minute))

	cfg := e.specs[id].cfg
	assert.Equal(t, 10*time.Minute, cfg.interval)
	assert.Equal(t, defaultCfg.timeout, cfg.timeout)
	assert.Equal(t, defaultCfg.backoff, cfg.backoff)
}

// --- handoff checklist ---
//
// Every skip below is a rule from docs/specs/probe-engine.md, with the kubeconn test to port
// named where one exists. Delete the skip as the behavior lands; a skip left behind is
// unfinished work. Porting means the same shape: block on channels via internal/testutil, pace
// by option (WithInterval/WithBackoff shrunk), never sleep.

func TestRegisterPanicsAfterStart(t *testing.T) {
	t.Skip("TODO(probe): needs Start — the probe set is fixed once the engine runs")
}

func TestAddQueuesWhatAFreshSubjectOwes(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAcquireAsksOnlyForANewContext — a fresh subject's first pass queues its runnable probes, and a second Add changes nothing")
}

func TestWorkQueuedBeforeStartIsServedOnceItRuns(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAClaimTakenBeforeStartIsChecked — the queues outlive the gap before Start")
}

func TestRemoveStopsTheSubjectsTimer(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestReleaseStopsTheScheduledRuns")
}

func TestARunAgainstARemovedSubjectCommitsNothing(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAReadIsNotCommittedToAReplacementClaim — remove mid-run, re-Add, and the stale commit must miss the new *subject")
}

func TestAWakeMidRunEarnsAFreshRun(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAnAskArrivingDuringAReadIsReadAgain — the run in flight had already passed the thing the Wake is about")
}

func TestAWakeRunsASuspendedProbe(t *testing.T) {
	t.Skip("TODO(probe): a Wake overrides suspension — it is the one input the derivation cannot produce")
}

func TestWakeAllReachesEveryTrackedSubject(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAKubeconfigChangeMakesTheConnectionCheckDue, over two subjects")
}

func TestARunInFlightSchedulesNothing(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestACheckInFlightSchedulesNothing — NextAttempt is that run; its commit passes again")
}

func TestADependentIsUntouchedBeforeItsNeedsAnswer(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestADependentIsNotDueBeforeTheConnectionHasAnswered — no record, nothing scheduled")
}

func TestADependentRecordsDependencyFailedOnceThenSuspends(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestACheckBehindTheConnectionSuspendsWhileItIsDown + TestADependentStaysSuspendedForTheRestOfAnOutage — one timeout per cycle, not per probe")
}

func TestARecoveredDependencyMakesItsDependentsDue(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAConnectionComingUpArmsWhatSuspendedOnIt — the re-arm is read off the state, nothing notices the recovery")
}

func TestNeedsIsRecheckedAtDispatch(t *testing.T) {
	t.Skip("TODO(probe): a dependency failing between the pass and the worker means DependencyFailed is recorded, not dialed")
}

func TestASucceededProbeIsDueAgainAfterItsInterval(t *testing.T) {
	t.Skip("TODO(probe): the success-path poll is the default; Suspend is the only opt-out")
}

func TestAFailedProbeClimbsTheLadder(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAResolveFailureKeepsAsking — and the ladder is a pure function of Failures, so the same state derives the same ScheduledAt on every pass")
}

func TestASkipLeavesNoRecordAndNothingScheduled(t *testing.T) {
	t.Skip("TODO(probe): the previous record stands, and only a Wake brings the probe back")
}

func TestTheTimerIsAWakeNotACadence(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestAScheduledCheckAsksForAReconcileWhenItIsDue — the timer queues a pass, and the pass decides")
}

func TestAPanickingRunRecordsInternalAndFreesItsKey(t *testing.T) {
	t.Skip("TODO(probe): otherwise it wedges twice over — in flight forever, and the key held in the queue")
}

func TestARunIsBoundedByItsTimeout(t *testing.T) {
	t.Skip("TODO(probe): the engine deadlines the ctx it hands the run; the body classifies the expiry itself")
}

func TestOnChangeFiresAfterEveryPassInOrderPerSubject(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestARunThatChangedNothingStillReachesAClaimWatcher — the caller decides what is news; the engine reports every pass")
}

func TestReadReportsAnUntrackedSubject(t *testing.T) {
	t.Skip("TODO(probe): ok is false, and a tracked subject's snapshot and attempts come from one lock hold")
}

func TestCloseStopsTheTimersAndTheQueues(t *testing.T) {
	t.Skip("TODO(probe): port kubeconn TestCloseStopsTheScheduledRuns + TestCloseDropsWhatThePoolHolds")
}
