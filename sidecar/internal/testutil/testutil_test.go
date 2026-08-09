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

package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The property the helper exists for: a fake whose callback runs many times can
// call Fire unguarded. Only the first call signals, and none of them panic.
func TestSignalFiresOnce(t *testing.T) {
	s := NewSignal()
	assert.True(t, s.Fire(), "the first Fire is the one that signals")
	assert.False(t, s.Fire())
	assert.False(t, s.Fire())
	s.Wait(t, "the signal")
}

// Fire before the test is waiting must still be observed: the value sits in the
// slot until Wait takes it, so there is no ordering requirement on a fake.
func TestSignalFireBeforeWait(t *testing.T) {
	s := NewSignal()
	s.Fire()
	s.Wait(t, "a signal fired before the wait")
	s.Wait(t, "a second wait, answered by the closed channel")
}

func TestSignalWaitsForALaterFire(t *testing.T) {
	s := NewSignal()
	go s.Fire()
	s.Wait(t, "a signal fired from another goroutine")
}

func TestWaitAcceptsCloseAndValue(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	Wait(t, closed, "a closed channel")

	sent := make(chan struct{}, 1)
	sent <- struct{}{}
	Wait(t, sent, "a delivered value")
}

func TestRecvReturnsTheValue(t *testing.T) {
	ch := make(chan int, 1)
	ch <- 42
	assert.Equal(t, 42, Recv(t, ch, "an int"))
}

func TestRecvClosedAcceptsAClosedChannel(t *testing.T) {
	ch := make(chan int)
	close(ch)
	RecvClosed(t, ch, "a closed channel")
}

// WaitClosed discards what is still in flight — a stream that closes with rows
// buffered has still ended.
func TestWaitClosedDrains(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 1
	ch <- 2
	close(ch)
	WaitClosed(t, ch, "a stream")
}
