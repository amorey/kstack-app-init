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
	"testing"

	"github.com/stretchr/testify/assert"
)

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

// A name nothing was registered under is a wiring bug on the untyped walk too, not a zero
// Attempts passed off as a probe that has never run.
func TestSnapshotAttemptsPanicsOnAnUnregisteredName(t *testing.T) {
	e, _, _ := single(t, Succeeded())
	e.Add(subj)
	read, _ := e.Read(subj)

	assert.Panics(t, func() { read.Attempts("nope") })
}
