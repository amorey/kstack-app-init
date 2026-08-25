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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var runAt = time.Date(2026, 8, 24, 10, 5, 0, 0, time.UTC)

func failedAt(at time.Time) Attempt {
	return Attempt{FinishedAt: at, Verdict: VerdictFailed, Reason: "Unreachable"}
}

func TestConsecutiveFailuresAreOneStreak(t *testing.T) {
	var a Attempts

	a.record(failedAt(runAt))
	a.record(failedAt(runAt.Add(time.Minute)))

	assert.Equal(t, 2, a.Failures)
	assert.Equal(t, runAt, a.FailingSince, "the streak began at the first failure")
	assert.False(t, a.OK())
}

func TestASuccessEndsTheStreak(t *testing.T) {
	a := Attempts{Failures: 3, FailingSince: runAt}

	a.record(Attempt{FinishedAt: runAt, Verdict: VerdictSucceeded, Reason: ReasonSucceeded})

	assert.True(t, a.OK())
	assert.Zero(t, a.Failures)
	assert.True(t, a.FailingSince.IsZero())
}

// A suspension parks a question rather than failing at it, so it ends a streak the way a
// success does — without claiming one.
func TestASuspensionEndsTheStreakWithoutASuccess(t *testing.T) {
	a := Attempts{Failures: 2, FailingSince: runAt}

	a.record(Attempt{FinishedAt: runAt.Add(time.Minute), Verdict: VerdictSuspended, Reason: "ContextNotFound"})

	assert.False(t, a.OK())
	assert.Zero(t, a.Failures)
	assert.True(t, a.FailingSince.IsZero())
}

func TestFailMessageDefaultsToTheError(t *testing.T) {
	r := Fail("ResolveFailed", errors.New("open ca.crt: no such file"))

	assert.Equal(t, VerdictFailed, r.verdict)
	assert.Equal(t, "open ca.crt: no such file", r.message)
}

func TestTheZeroResultIsInvalid(t *testing.T) {
	assert.Equal(t, resultInvalid, Result{}.kind, "constructors are the only way to a valid Result")
	assert.NotEqual(t, resultInvalid, Skip().kind)
}

// The read side is what a probe body's own tests assert on, without giving a body a way to build
// a Result the constructors cannot.
func TestAResultReportsWhatItWasBuiltFrom(t *testing.T) {
	err := errors.New("open ca.crt: no such file")

	failed := Fail("ResolveFailed", err)
	assert.Equal(t, VerdictFailed, failed.Verdict())
	assert.Equal(t, Reason("ResolveFailed"), failed.Reason())
	assert.Equal(t, err.Error(), failed.Message())
	assert.ErrorIs(t, failed.Err(), err)
	assert.False(t, failed.IsSkip())

	suspended := Suspend("ContextNotFound", "the file no longer names it")
	assert.Equal(t, VerdictSuspended, suspended.Verdict())
	assert.Equal(t, "the file no longer names it", suspended.Message())
	assert.NoError(t, suspended.Err())

	assert.True(t, Skip().IsSkip())
	assert.Equal(t, ReasonSucceeded, Succeeded().Reason())
}

// Latency is the run's own time, which only a dispatched run that finished has: a run still out
// has none to report, and one the engine recorded without dispatching never started.
func TestLatencyIsOnlyForARunThatWasDispatchedAndFinished(t *testing.T) {
	assert.Zero(t, Attempt{StartedAt: runAt}.Latency(), "still running")
	assert.Zero(t, Attempt{FinishedAt: runAt}.Latency(), "recorded, never dispatched")

	ran := Attempt{StartedAt: runAt, FinishedAt: runAt.Add(2 * time.Second)}
	assert.Equal(t, 2*time.Second, ran.Latency())
}
