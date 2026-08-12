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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// Every pump ends its goroutine on a false return, so the ctx-cancel arm is what stops
// a stream whose subscriber left mid-send. Unbuffered: the send is parked with no
// receiver, exactly the case that would otherwise leak the goroutine for the process's
// life.
func TestSendReturnsFalseWhenTheSubscriberLeaves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan int)

	done := testutil.NewSignal()
	var ok bool
	go func() {
		ok = send(ctx, out, 1)
		done.Fire()
	}()

	cancel()
	done.Wait(t, "the parked send to give up")
	assert.False(t, ok)
}

func TestSendDeliversToAWaitingReceiver(t *testing.T) {
	out := make(chan int, 1)
	require.True(t, send(context.Background(), out, 7))
	assert.Equal(t, 7, <-out)
}
