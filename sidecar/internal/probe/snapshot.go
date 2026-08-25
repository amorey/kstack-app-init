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

// The read side: what a caller — and every Run — sees of a subject's observables.
package probe

import (
	"fmt"
	"time"
)

// Snapshot is one subject's observables, copied under the engine's lock so the values and the
// schedule beside them agree. It is frozen at the moment it was taken — a run holding one can
// have it go stale under it, which is what the data edge exists to answer. Get is the typed
// read, by registration name; Attempts is the untyped one, for a reader walking every probe.
type Snapshot struct {
	obs []observable
	// byName is the engine's index of which probe a name addresses, shared rather than copied:
	// registration is closed before any subject exists, so nothing writes it once a Snapshot
	// can be taken.
	byName map[string]ID
}

// Attempts is probe id's bookkeeping. Panics on an ID never registered, like any index.
func (snap Snapshot) Attempts(id ID) Attempts { return snap.obs[id].Attempts }

// Len is how many probes are registered, so ID(0)..ID(Len-1) index Attempts.
func (snap Snapshot) Len() int { return len(snap.obs) }

// Get is one probe's observation, by the name it was registered under: the value its runs
// committed — the zero T until one has, which Known reports — beside the attempts that account
// for it. This is how a Run reads a sibling, and it needs nothing wired to do it.
//
// It panics on a name nothing was registered under, and on a T that is not what the probe
// commits: both are wiring bugs, and a zero value handed back as an answer is worse than a
// stopped process — a caller's "nothing known yet" branch would swallow it and park the probe
// for good. A type is only checkable against a value that exists, so a mistyped read of a probe
// that has committed nothing reads as not Known, and panics as soon as one lands.
func Get[T any](snap Snapshot, name string) Observation[T] {
	id, ok := snap.byName[name]
	if !ok {
		panic("probe: no probe named " + name)
	}

	src := snap.obs[id]
	o := Observation[T]{LastSeen: src.seen, Attempts: src.Attempts}
	if src.value == nil {
		return o
	}
	val, ok := src.value.(T)
	if !ok {
		panic(fmt.Sprintf("probe: %q observes %T, read as %T", name, src.value, o.Value))
	}
	o.Value = val
	return o
}

// Observation is one probe's last answer and the provenance to judge it: the committed value
// beside the engine's bookkeeping for the probe that read it.
//
// Value outlives the failure that follows it — a read that stops being permitted does not mean
// the fact changed — and LastSeen is what makes the survivor readable: "identified, as of 10:00"
// is usable where "ready, as of 10:00" is not. A probe that has never run is the zero value,
// which needs no sentinel: a zero LastAttempt is not Done, so every accessor already answers
// for it.
type Observation[T any] struct {
	// Value is the last committed answer. Meaningless until Known.
	Value T
	// LastSeen is when a run last confirmed Value: every success that has a value to date
	// advances it, whether or not that run committed a new one. Zero while there is no value,
	// which is what Known reads.
	LastSeen time.Time

	Attempts
}

// Known reports whether a run has committed a value, which is what makes Value readable. A
// succeeded run is not enough on its own — one can conclude nothing to write.
func (o Observation[T]) Known() bool { return !o.LastSeen.IsZero() }
