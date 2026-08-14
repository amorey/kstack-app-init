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

package cluster

import (
	"sync/atomic"
)

// Stream is what a watch whose source can die returns: the frames, and why they
// stopped. Frames closes on every exit — ctx cancelled, source drained, source
// failed — so Err is the only thing that tells a failure apart from an ordinary
// teardown, and a consumer that ignores it turns a broken watch into a silent one.
//
// The rule is the SOURCE, not the shape: anything reading a beehive watch returns a
// Stream, gauges included (WatchSyncHealth folds two of its own). The Data watches
// are the only ones that don't, because they read the local cache and retry forever
// — cacheWatchLoop has no terminal failure to report.
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
// terminal error. The frame channel closes only after that error is recorded,
// which is what makes "Frames closed" a safe cue to read Err.
//
// Exported because Stream sits in the family interfaces: anything implementing
// them — a fake in the resolver tests, say — has to be able to build one.
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
