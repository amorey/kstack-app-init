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

package clustersvc

import (
	"sync/atomic"
)

// Stream is what a watch whose source can die returns: the frames, and why they
// stopped. Frames closes on every exit — ctx cancelled, source drained, source
// failed — so Err is the only thing that tells a failure apart from an ordinary
// teardown, and a consumer that ignores it turns a broken watch into a silent one.
//
// The rule is the SOURCE, not the shape: a watch over a fallible upstream returns a
// Stream, gauges included. One that retries forever and has no terminal failure to
// report may stay a plain channel — which is why the Data family does.
type Stream[T any] struct {
	Frames <-chan T
	err    atomic.Pointer[error]
}

// Err returns why the stream ended, or nil if it ended cleanly. Read it once
// Frames has closed; before that it is always nil.
func (s *Stream[T]) Err() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}

// NewStream runs pump on its own goroutine, publishing what it returns as the
// terminal error. Frames closes only after that error is recorded, which is what
// makes "Frames closed" a safe cue to read Err.
//
// Exported because Stream sits in the family interfaces: an implementation outside
// this package — a fake in the resolver tests — has to be able to build one.
func NewStream[T any](pump func(out chan<- T) error) *Stream[T] {
	ch := make(chan T, 1)
	s := &Stream[T]{Frames: ch}
	go func() {
		defer close(ch)
		if err := pump(ch); err != nil {
			s.err.Store(&err)
		}
	}()
	return s
}
