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
	"errors"
	"testing"
	"time"

	"github.com/amorey/gochan/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

var runAt = time.Date(2026, 8, 23, 10, 5, 0, 0, time.UTC)

// fakeKubeconfig resolves one context to a fixed answer, standing in for the user's file.
type fakeKubeconfig struct {
	key string
	err error
	hub *watch.Hub[*api.Config]
}

func (f *fakeKubeconfig) RESTConfig(string) (*rest.Config, string, error) {
	if f.err != nil {
		return nil, "", f.err
	}
	return &rest.Config{Host: "https://prod.example:6443"}, f.key, nil
}

func (f *fakeKubeconfig) Subscribe() kubeconfig.Subscription {
	if f.hub == nil {
		f.hub = watch.New[*api.Config](nil)
	}
	return f.hub.Receiver()
}

// A check the prober records without dispatching has no duration: subtracting a zero
// StartedAt would report the time since the zero year.
func TestLatencyIsZeroForARunThatNeverStarted(t *testing.T) {
	a := Attempt{ScheduledAt: runAt, FinishedAt: runAt, Reason: ReasonDependencyFailed}

	assert.True(t, a.Done())
	assert.False(t, a.Running())
	assert.Zero(t, a.Latency())
}

func TestLatencyMeasuresADispatchedRun(t *testing.T) {
	a := Attempt{ScheduledAt: runAt, StartedAt: runAt, FinishedAt: runAt.Add(2 * time.Second)}

	assert.Equal(t, 2*time.Second, a.Latency())
}

// A suspended check keeps its last answer and schedules nothing.
func TestSuspendedCheckKeepsItsAnswer(t *testing.T) {
	o := Observation[string]{
		Value:       "abc-123",
		LastSeen:    runAt,
		LastAttempt: Attempt{ScheduledAt: runAt, FinishedAt: runAt, Reason: ReasonDependencyFailed},
	}

	assert.True(t, o.Known(), "the UID it read is still the UID it read")
	assert.False(t, o.OK())
	assert.False(t, o.Scheduled(), "nothing is due until the connection comes back")
	assert.False(t, o.InFlight())
}

// Identity projects the three scalars a connection is scoped to out of the observations
// carrying them, so retiring one stays a ==.
func TestIdentityProjectsTheCheckedScalars(t *testing.T) {
	s := State{
		ServerUID:     Observation[string]{Value: "uid-1", LastSeen: runAt},
		ServerVersion: Observation[VersionInfo]{Value: VersionInfo{GitVersion: "v1.29.3"}, LastSeen: runAt},
		Principal: Observation[Principal]{
			Value:    Principal{Username: "admin@example", Groups: []string{"system:masters"}},
			LastSeen: runAt,
		},
	}

	assert.Equal(t, Identity{
		ServerUID:     "uid-1",
		ServerVersion: "v1.29.3",
		Username:      "admin@example",
	}, s.Identity())
}

// A part no probe could read is empty rather than absent, which is what lets two
// connections missing the same part compare equal.
func TestIdentityLeavesAnUnreadPartEmpty(t *testing.T) {
	forbidden := State{
		ServerUID: Observation[string]{LastAttempt: Attempt{FinishedAt: runAt, Reason: ReasonForbidden}},
		Principal: Observation[Principal]{Value: Principal{Username: "reader@example"}, LastSeen: runAt},
	}

	assert.Equal(t, Identity{Username: "reader@example"}, forbidden.Identity())
	assert.Equal(t, forbidden.Identity(), State{
		ServerUID: Observation[string]{LastAttempt: Attempt{FinishedAt: runAt, Reason: ReasonUnsupported}},
		Principal: Observation[Principal]{Value: Principal{Username: "reader@example"}, LastSeen: runAt},
	}.Identity(), "why the UID is missing is the observation's, not the identity's")
}

// The pre-read kubeconfig is empty, so every context looks departed. Refusing the claim
// would report a live cluster's credentials broken for as long as the first read takes.
func TestAcquireClaimsAContextTheKubeconfigHasNotReadYet(t *testing.T) {
	s := New(&fakeKubeconfig{err: kubeconfig.ErrNotRead})

	lease, err := s.Acquire("prod")

	require.NoError(t, err)
	assert.NotNil(t, lease)
}

// A context the file genuinely does not resolve is refused with the reader's own error,
// rather than becoming a claim on credentials nobody could build.
func TestAcquireRefusesAContextThatWillNotResolve(t *testing.T) {
	boom := errors.New("no such certificate authority file")
	s := New(&fakeKubeconfig{err: boom})

	_, err := s.Acquire("prod")

	assert.ErrorIs(t, err, boom)
}

// The subscription is established before Start returns, so a config sent straight after it
// is read rather than dropped, and stop joins the loop instead of leaving it running.
func TestStartWatchesTheKubeconfigUntilStopped(t *testing.T) {
	cfg := &fakeKubeconfig{key: "key-1"}
	s := New(cfg)

	stop, err := s.Start(t.Context())
	require.NoError(t, err)
	require.NotNil(t, cfg.hub, "Start subscribes before it returns")
	cfg.hub.Sender().Send(&api.Config{})

	require.NoError(t, stop(t.Context()))
}
