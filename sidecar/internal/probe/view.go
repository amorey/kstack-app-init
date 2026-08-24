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

// View is one subject's observables, copied under the engine's lock so the values and the
// schedule beside them agree. The typed read goes through each probe's Handle; Attempts is the
// untyped one, for a reader walking every probe.
type View struct {
	values   []any
	attempts []Attempts
}

// Attempts is probe id's bookkeeping. Panics on an ID never registered, like any index.
func (v View) Attempts(id ID) Attempts { return v.attempts[id] }

// Len is how many probes are registered, so ID(0)..ID(Len-1) index Attempts.
func (v View) Len() int { return len(v.attempts) }

// Handle is a probe's typed registration, returned by Register: the ID that Needs, Wake, and
// View.Attempts address the probe by, and the typed read of its observable.
type Handle[T any] struct {
	id   ID
	name string
}

// ID is the probe's registration index.
func (h Handle[T]) ID() ID { return h.id }

// Name is the name the probe was registered under.
func (h Handle[T]) Name() string { return h.name }

// Get is this probe's observation out of a View: the value its runs committed (the zero T until
// one has) beside the engine's attempts.
func (h Handle[T]) Get(v View) Observation[T] {
	o := Observation[T]{Attempts: v.attempts[h.id]}
	if val := v.values[h.id]; val != nil {
		o.Value = val.(T)
	}
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

	Attempts
}

// Known reports whether this probe has ever answered, which is what makes Value readable.
func (o Observation[T]) Known() bool { return !o.LastSeen.IsZero() }
