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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// A consumer learns a watch died by seeing Frames close and then reading Err, so the
// reason must already be recorded by the time the close is observable. Were the order
// reversed, a resolver would read nil on the losing side of the race and report a
// broken watch as a graceful end — the exact failure this type exists to prevent.
func TestStreamRecordsTheReasonBeforeClosingFrames(t *testing.T) {
	boom := errors.New("watch ended: too old")
	s := NewStream(context.Background(), func(_ context.Context, out chan<- int) error {
		out <- 1
		return boom
	})

	require.Equal(t, 1, testutil.Recv(t, s.Frames, "a stream value"))
	testutil.WaitClosed(t, s.Frames, "the stream")
	assert.Equal(t, boom, s.Err())
}

func TestStreamReportsNoErrorOnACleanEnd(t *testing.T) {
	s := NewStream(context.Background(), func(context.Context, chan<- int) error { return nil })

	testutil.WaitClosed(t, s.Frames, "the stream")
	assert.NoError(t, s.Err())
}

// The whole of what sendFrame is for. A bare channel send would park here forever,
// leaking the pump goroutine and the store watch behind it once per client disconnect —
// and reporting nothing, since Frames never closes.
func TestSendFrameGivesUpWhenTheConsumerIsGone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.False(t, sendFrame(ctx, make(chan int), 1), "nothing is draining out")
}

// The shape every pump owes its consumer: the reader stops draining Frames when ctx
// ends, so a pump that does not select on it blocks forever on its next send. A
// cancelled watch is a teardown, not a failure, so Err stays nil.
func TestStreamPumpEndsOnACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	s := NewStream(ctx, func(ctx context.Context, out chan<- int) error {
		for {
			select {
			case out <- 1:
			case <-ctx.Done():
				return nil
			}
		}
	})

	// Fill the buffer and leave a send parked, so cancellation has to be what
	// releases the pump rather than an empty channel.
	require.Equal(t, 1, testutil.Recv(t, s.Frames, "a stream value"))
	cancel()

	testutil.WaitClosed(t, s.Frames, "the stream")
	assert.NoError(t, s.Err(), "a cancelled watch is a teardown, not a failure")
}
