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

package kubeconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The seam the probe fills: a holder that asks for a connection gets a clear answer rather than
// a nil it has to interpret.
func TestConnReportsThatNothingBuildsOneYet(t *testing.T) {
	s := New(resolving("prod", "key-1"))
	lease := s.Acquire("prod")
	defer lease.Release()

	conn, err := lease.Conn(t.Context())

	assert.Nil(t, conn)
	assert.ErrorIs(t, err, ErrNoConnection)
}

// A connection nobody built has a nil channel, which blocks forever — reading as never retired,
// where a closed one would tell every holder its connection had gone.
func TestDoneOnAConnectionNobodyBuiltNeverFires(t *testing.T) {
	var c Connection

	select {
	case <-c.Done():
		t.Fatal("an unbuilt connection reported itself retired")
	default:
	}

	retired := Connection{done: make(chan struct{})}
	close(retired.done)
	<-retired.Done()
}
