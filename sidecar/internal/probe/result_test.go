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

func TestASuccessEndsTheStreakAndStampsLastSeen(t *testing.T) {
	a := Attempts{Failures: 3, FailingSince: runAt}

	a.record(Attempt{FinishedAt: runAt, Verdict: VerdictSucceeded, Reason: ReasonSucceeded})

	assert.True(t, a.OK())
	assert.Zero(t, a.Failures)
	assert.True(t, a.FailingSince.IsZero())
	assert.Equal(t, runAt, a.LastSeen, "the engine stamps when the value was read")
}

// A suspension parks a question rather than failing at it, so it ends a streak the way a
// success does — without claiming one.
func TestASuspensionEndsTheStreakWithoutASuccess(t *testing.T) {
	a := Attempts{Failures: 2, FailingSince: runAt, LastSeen: runAt}

	a.record(Attempt{FinishedAt: runAt.Add(time.Minute), Verdict: VerdictSuspended, Reason: "ContextNotFound"})

	assert.False(t, a.OK())
	assert.Zero(t, a.Failures)
	assert.True(t, a.FailingSince.IsZero())
	assert.Equal(t, runAt, a.LastSeen, "no success, so LastSeen stands")
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
