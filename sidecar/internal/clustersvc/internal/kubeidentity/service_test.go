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

package kubeidentity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// The shape the cluster service composes this into.
var _ lifecycle.StartCloser = (*Service)(nil)

// An unprobed context reports nothing known rather than an empty identity, which is the
// distinction a caller renders: "connecting" is not "connected to a server with no UID".
// Every context reads this way until there are workers.
func TestGetReportsNothingKnown(t *testing.T) {
	state, known := New().Get("prod")

	assert.False(t, known)
	assert.Equal(t, State{}, state)
}

// Get is the read a reconcile pass makes, so it must answer before anything is started:
// a pass reaching a service still starting would otherwise block or panic where it is
// documented to do neither.
func TestGetAnswersBeforeStart(t *testing.T) {
	s := New()

	_, known := s.Get("prod")
	assert.False(t, known)

	stop, err := s.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
	assert.NoError(t, s.Close())
}

// The notifier subscribes at startup and parks on this, so it has to hand back a usable
// receiver long before anything sends.
func TestSubscribeIsUsableBeforeAnythingSends(t *testing.T) {
	sub := New().Subscribe()
	defer sub.Close()

	require.NotNil(t, sub)
	assert.NotNil(t, sub.Chan())
}
