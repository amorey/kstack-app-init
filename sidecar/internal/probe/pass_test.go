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

// A body reads its inputs off the pass, and a body with no news leaves it alone.
func TestAPassCarriesTheRunsInputsAndRecordsNothingByDefault(t *testing.T) {
	p := NewPass("ctx-1", "v0", Snapshot{})

	assert.Equal(t, "ctx-1", p.Subject())
	assert.Equal(t, "v0", p.Prev())

	_, ok := p.Updated()
	assert.False(t, ok, "a run that said nothing recorded nothing")
}

// One run, one value: a body that learns twice records the second, so the engine has one thing
// to apply and the watchers one thing to wake for.
func TestTheLastCommitOnAPassWins(t *testing.T) {
	p := NewPass("ctx-1", "v0", Snapshot{})

	p.Commit("v1")
	p.Commit("v2")

	got, ok := p.Updated()
	assert.True(t, ok)
	assert.Equal(t, "v2", got)
}

// The snapshot the run was dispatched against comes back off the pass — how a body reaches a
// sibling's value without anything being wired into it.
func TestAPassCarriesTheSnapshotItWasHanded(t *testing.T) {
	e, body, _ := single(t, Succeeded())
	body.commits("v1")
	e.Add(subj)
	e.settle()
	runNext(t, e)
	read, _ := e.Read(subj)

	p := NewPass("ctx-1", "v0", read)

	assert.Equal(t, "v1", Get[string](p.Snapshot(), "conn").Value)
}
