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
	s := NewStream(func(out chan<- int) error {
		out <- 1
		return boom
	})

	require.Equal(t, 1, testutil.Recv(t, s.Frames, "a stream value"))
	testutil.WaitClosed(t, s.Frames, "the stream")
	assert.Equal(t, boom, s.Err())
}

func TestStreamReportsNoErrorOnACleanEnd(t *testing.T) {
	s := NewStream(func(chan<- int) error { return nil })

	testutil.WaitClosed(t, s.Frames, "the stream")
	assert.NoError(t, s.Err())
}
