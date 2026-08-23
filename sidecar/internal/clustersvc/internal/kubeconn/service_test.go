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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var runAt = time.Date(2026, 8, 23, 10, 5, 0, 0, time.UTC)

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
