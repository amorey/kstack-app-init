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
package probe

// Pass is one run of one probe: what it is against, what it knew going in, and where it records
// what it found. The engine builds one per run and reads it back when the run returns.
type Pass[T any] struct {
	subject string
	prev    T
	snap    Snapshot
	next    *T
}

// NewPass builds a pass for a probe body's own tests, standing in for the engine.
func NewPass[T any](subject string, prev T, snap Snapshot) *Pass[T] {
	return &Pass[T]{subject: subject, prev: prev, snap: snap}
}

// Subject is the subject this run is against.
func (p *Pass[T]) Subject() string { return p.subject }

// Prev is this probe's own last committed value, the zero T until one lands.
func (p *Pass[T]) Prev() T { return p.prev }

// Snapshot is every probe's observable as of dispatch, for sibling reads through Get.
func (p *Pass[T]) Snapshot() Snapshot { return p.snap }

// Commit records v as what this run found, callable wherever the body learns it. The engine
// buffers it and applies it when the run returns, in the same critical section as the attempt —
// so nothing is published mid-run, and a run that then panics or concludes Skip commits nothing.
//
// **It asserts the value moved**, and the engine takes that at its word: committing a value
// equal to the last one re-runs every probe watching this one against news that is not news.
// Equality is the body's to judge, since the engine holds values as any and a probe's value may
// be uncomparable or carry funcs that no generic compare can read.
//
// Calling it twice commits the second value: one run, one attempt, one wake.
func (p *Pass[T]) Commit(v T) { p.next = &v }

// Updated is what the body recorded and whether it did — the read side, for a probe body's own
// tests, as Result's accessors are.
func (p *Pass[T]) Updated() (T, bool) {
	if p.next == nil {
		var zero T
		return zero, false
	}
	return *p.next, true
}
