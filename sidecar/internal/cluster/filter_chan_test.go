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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterChanForwardsOnlyKept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan int, 4)
	for _, v := range []int{1, 2, 3, 4} {
		in <- v
	}
	close(in)

	out := filterChan(ctx, in, func(v int) []int {
		if v%2 == 0 {
			return []int{v}
		}
		return nil
	})

	var got []int
	for v := range out {
		got = append(got, v)
	}
	assert.Equal(t, []int{2, 4}, got)
}

// The consumer of a per-cache watch stops draining the moment its client goes away — a
// closed sync dialog — and only then is the stream's ctx cancelled. A bare channel send
// cannot be unblocked by that cancel, so the goroutine would park forever holding the value
// it was mid-forward on: one leak per open/close of the dialog, for the process lifetime.
func TestFilterChanUnblocksAParkedSendOnContextEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan int)
	out := filterChan(ctx, in, func(v int) []int { return []int{v} })

	// Fill the single slot, then leave another value parked in the send with nobody
	// draining — exactly the shape of an abandoned subscription.
	in <- 1
	in <- 2
	require.Eventually(t, func() bool { return len(out) == 1 }, time.Second, 5*time.Millisecond)

	// The upstream watch unwinds on the same ctx, so close in as watchListChan does. What
	// is under test is the goroutine parked in the SEND: closing the input cannot reach it.
	close(in)
	cancel()

	// The goroutine must unwind and close out. Drain rather than counting: once the
	// buffered value is taken the parked send has room again, so whether it delivers or
	// sees the cancel is a genuine race in select — what must NOT happen is the goroutine
	// staying parked, which shows up as out never closing.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return // closed: the goroutine unwound
			}
		case <-deadline:
			t.Fatal("filterChan leaked a goroutine parked in a send")
		}
	}
}
