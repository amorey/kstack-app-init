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

// One run's inputs and what it records. Pure data — nothing here reaches the queues or the
// subjects.
package supervisor

// Pass is one run of one reconciler: what it is against, what it knew going in, and where it records
// what it found. The supervisor builds one per run and reads it back when the run returns.
type Pass[T any] struct {
	subject string
	prev    T
	known   bool
	snap    Snapshot
	next    *T
}

// NewPass builds a pass for a reconciler body's own tests, standing in for the supervisor. prev is what
// the reconciler last committed, or nil for one that has committed nothing yet.
func NewPass[T any](subject string, prev *T, snap Snapshot) *Pass[T] {
	p := &Pass[T]{subject: subject, snap: snap}
	if prev != nil {
		p.prev, p.known = *prev, true
	}
	return p
}

// Subject is the subject this run is against.
func (p *Pass[T]) Subject() string { return p.subject }

// Prev is this reconciler's own last committed value, the zero T until one lands.
func (p *Pass[T]) Prev() T { return p.prev }

// Known reports whether a value has ever been committed. What a reconciler whose zero T is a
// legitimate answer needs: Prev cannot tell "nothing has landed" from "the last answer was the
// zero value", so without this such a reconciler never commits its first answer and reads as never
// observed for as long as it stays healthy.
func (p *Pass[T]) Known() bool { return p.known }

// Snapshot is every reconciler's observable as of dispatch, for sibling reads through Get.
func (p *Pass[T]) Snapshot() Snapshot { return p.snap }

// Commit records v as what this run found, callable wherever the body learns it. The supervisor
// buffers it and applies it when the run returns, in the same critical section as the attempt —
// so nothing is published mid-run, and a run that then panics or concludes Skip commits nothing.
//
// **It asserts the value moved**, and the supervisor takes that at its word: committing a value
// equal to the last one re-runs every reconciler watching this one against news that is not news.
// Equality is the body's to judge, since the supervisor holds values as any and a reconciler's value may
// be uncomparable or carry funcs that no generic compare can read.
//
// Calling it twice commits the second value: one run, one attempt, one wake.
func (p *Pass[T]) Commit(v T) { p.next = &v }

// Updated is what the body recorded and whether it did — the read side, for a reconciler body's own
// tests, as Result's accessors are.
func (p *Pass[T]) Updated() (T, bool) {
	if p.next == nil {
		var zero T
		return zero, false
	}
	return *p.next, true
}
