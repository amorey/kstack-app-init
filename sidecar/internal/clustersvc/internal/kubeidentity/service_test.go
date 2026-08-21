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
	"errors"
	"fmt"
	"testing"

	"github.com/amorey/gobus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// The shape the cluster service composes this into.
var _ lifecycle.StartCloser = (*Service)(nil)

// fakeKubeconfig resolves the contexts it holds and reports every other one departed, which is
// what the real service does. The map is rewritten mid-test to re-point a context.
type fakeKubeconfig struct {
	keys map[string]string
	err  error
	// asked records every context resolved, in order.
	asked []string
}

func (f *fakeKubeconfig) RESTConfig(contextName string) (*rest.Config, string, error) {
	f.asked = append(f.asked, contextName)
	if f.err != nil {
		return nil, "", f.err
	}
	key, ok := f.keys[contextName]
	if !ok {
		return nil, "", fmt.Errorf("%w: %q", kubeconfig.ErrContextNotFound, contextName)
	}
	return &rest.Config{Host: "https://" + contextName + ".example"}, key, nil
}

// serviceOver returns a service over the given context→key mapping.
func serviceOver(t *testing.T, keys map[string]string) (*Service, *fakeKubeconfig) {
	t.Helper()
	kubecfg := &fakeKubeconfig{keys: keys}
	s := New(kubecfg)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, kubecfg
}

// A context nothing has dialed reports nothing known rather than an empty identity, which is
// the distinction a caller renders: "connecting" is not "connected to a server with no UID".
func TestGetReportsNothingKnown(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{"prod": "key-1"})

	state, known := s.Get("prod")

	assert.False(t, known)
	assert.Equal(t, State{}, state)
}

// A context that will not resolve is known — that is the answer — and carries a sentinel the
// caller acts on.
func TestGetReportsADepartedContext(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{})

	state, known := s.Get("prod")

	assert.True(t, known)
	assert.ErrorIs(t, state.Err, kubeconfig.ErrContextNotFound)
}

// The unread config is empty, so every context looks departed. Reporting that would record a
// live cluster as gone.
func TestGetReportsNothingKnownWhileTheKubeconfigIsUnread(t *testing.T) {
	s, kubecfg := serviceOver(t, map[string]string{"prod": "key-1"})
	kubecfg.err = kubeconfig.ErrNotRead

	state, known := s.Get("prod")

	assert.False(t, known)
	assert.Equal(t, State{}, state)
}

// A file that will not resolve at all is neither of the two above: the caller reports it as
// something the user has to fix, so it must arrive as an error rather than as nothing known.
func TestGetReportsAFailedResolve(t *testing.T) {
	s, kubecfg := serviceOver(t, map[string]string{"prod": "key-1"})
	kubecfg.err = errors.New("no such certificate authority file")

	state, known := s.Get("prod")

	assert.True(t, known)
	assert.ErrorContains(t, state.Err, "certificate authority")
}

// The resolve is per ask, never memoized: it is what tells the credentials a stored answer
// was learned under from the ones the context names now.
func TestGetResolvesEveryTime(t *testing.T) {
	s, kubecfg := serviceOver(t, map[string]string{"prod": "key-1"})

	s.Get("prod")
	s.Get("prod")

	assert.Equal(t, []string{"prod", "prod"}, kubecfg.asked)
}

// Get is the read a reconcile pass makes, so it must answer before anything is started: a pass
// reaching a service still starting would otherwise block or panic where it is documented to
// do neither.
func TestGetAnswersBeforeStart(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{"prod": "key-1"})

	_, known := s.Get("prod")
	assert.False(t, known)

	stop, err := s.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
}

// lifecycle calls a stop func more than once on an unwind, and Close follows it, so both must
// be no-ops the second time rather than a panic or a hang.
func TestStopAndCloseAreIdempotent(t *testing.T) {
	s := New(&fakeKubeconfig{keys: map[string]string{}})
	stop, err := s.Start(context.Background())
	require.NoError(t, err)

	require.NoError(t, stop(context.Background()))
	require.NoError(t, stop(context.Background()))
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

// The trigger subscribes at startup and parks on this, so it has to hand back a usable
// receiver long before anything sends.
func TestSubscribeIsUsableBeforeAnythingSends(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{})

	sub := s.Subscribe()
	defer sub.Close()

	require.NotNil(t, sub)
	assert.NotNil(t, sub.Chan())
}

// published returns the contexts the service has announced since the last read. Reading is a
// cut of the queue rather than a receive, so "nothing was announced" is an empty slice now,
// not the absence of an event within some window.
func published(t *testing.T, sub Subscription) []string {
	t.Helper()
	evs, err := sub.TryRecvAll()
	if !errors.Is(err, gobus.ErrEmpty) {
		require.NoError(t, err)
	}
	names := make([]string, 0, len(evs))
	for _, ev := range evs {
		names = append(names, ev.Key)
	}
	return names
}

// The ordinary read: a probe answered, the context still names those credentials, so the
// answer is current.
func TestGetReportsWhatTheProbeRecorded(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{"prod": "key-1"})
	s.record("prod", "key-1", State{Identity: Identity{ServerUID: "uid-1"}})

	state, known := s.Get("prod")

	assert.True(t, known)
	assert.Equal(t, "uid-1", state.Identity.ServerUID)
}

// The whole of what the key is for. A kubeconfig edit that rotates a token or re-points the
// server leaves an identity behind that describes a connection nobody would make again — and
// nothing else in the process could tell, since the probe that would overwrite it has not run.
// Reporting it would mark a cluster connected to a server these credentials no longer reach.
func TestGetReportsNothingKnownOnceTheCredentialsMove(t *testing.T) {
	s, kubecfg := serviceOver(t, map[string]string{"prod": "key-1"})
	s.record("prod", "key-1", State{Identity: Identity{ServerUID: "uid-1"}})

	kubecfg.keys["prod"] = "key-2"

	state, known := s.Get("prod")

	assert.False(t, known)
	assert.Equal(t, State{}, state)
}

// A probe failure is an answer, so it is stored and read back like any other — the caller
// reports it rather than waiting on a cluster that has already said no.
func TestGetReportsARecordedProbeFailure(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{"prod": "key-1"})
	s.record("prod", "key-1", State{Err: errors.New("dial tcp: connection refused")})

	state, known := s.Get("prod")

	assert.True(t, known)
	assert.ErrorContains(t, state.Err, "connection refused")
}

// One context's answer says nothing about another's, and the signal must name only the one
// that moved: a shared slot would wake the whole fleet for one cluster.
func TestRecordKeepsContextsApart(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{"prod": "key-1", "staging": "key-2"})
	s.record("prod", "key-1", State{Identity: Identity{ServerUID: "uid-1"}})

	_, known := s.Get("staging")

	assert.False(t, known)
}

func TestRecordPublishes(t *testing.T) {
	tests := []struct {
		name  string
		first *State
		next  State
		want  []string
	}{
		{
			name: "a first answer",
			next: State{Identity: Identity{ServerUID: "uid-1"}},
			want: []string{"prod"},
		},
		{
			name:  "an identity that moved",
			first: &State{Identity: Identity{ServerUID: "uid-1"}},
			next:  State{Identity: Identity{ServerUID: "uid-2"}},
			want:  []string{"prod"},
		},
		{
			// The cost of a signal is the cluster's pass plus its record re-emitted to every
			// watcher, so the re-probe cadence must be silent when the server has not moved.
			name:  "nothing at all",
			first: &State{Identity: Identity{ServerUID: "uid-1"}},
			next:  State{Identity: Identity{ServerUID: "uid-1"}},
			want:  []string{},
		},
		{
			// Rebuilt per attempt, so comparing errors by value would publish every cadence
			// for a cluster that is steadily, unchangingly down.
			name:  "the same failure twice",
			first: &State{Err: errors.New("connection refused")},
			next:  State{Err: errors.New("connection refused")},
			want:  []string{},
		},
		{
			name:  "a failure that reads differently",
			first: &State{Err: errors.New("connection refused")},
			next:  State{Err: errors.New("certificate expired")},
			want:  []string{"prod"},
		},
		{
			name:  "a cluster that went down",
			first: &State{Identity: Identity{ServerUID: "uid-1"}},
			next:  State{Err: errors.New("connection refused")},
			want:  []string{"prod"},
		},
		{
			// Reaching the server and reading nothing off it is still reaching it, and the
			// identity alone cannot say so: an empty one is what a failure carries too.
			name:  "a probe that answered with an empty identity",
			first: &State{Err: errors.New("connection refused")},
			next:  State{},
			want:  []string{"prod"},
		},
		{
			name:  "a cluster that came back",
			first: &State{Err: errors.New("connection refused")},
			next:  State{Identity: Identity{ServerUID: "uid-1"}},
			want:  []string{"prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := serviceOver(t, map[string]string{"prod": "key-1"})
			sub := s.Subscribe()
			defer sub.Close()

			if tt.first != nil {
				s.record("prod", "key-1", *tt.first)
				published(t, sub)
			}
			s.record("prod", "key-1", tt.next)

			assert.Equal(t, tt.want, published(t, sub))
		})
	}
}

// Same server, different credentials: the answer did not move, but Get reported nothing known
// for the stretch between the edit and this probe, so the caller is holding "connecting" and
// nothing but a signal takes it off that.
func TestRecordPublishesWhenOnlyTheCredentialsMoved(t *testing.T) {
	s, _ := serviceOver(t, map[string]string{"prod": "key-1"})
	sub := s.Subscribe()
	defer sub.Close()

	id := State{Identity: Identity{ServerUID: "uid-1"}}
	s.record("prod", "key-1", id)
	published(t, sub)

	s.record("prod", "key-2", id)

	assert.Equal(t, []string{"prod"}, published(t, sub))
}

// The probe resolves the credentials it is about to dial and hands that key back here, so a
// file that moves mid-probe cannot make the answer look current: the key it was learned under
// is the one stored, and the next Get refutes it.
func TestRecordStoresTheKeyTheProbeDialed(t *testing.T) {
	s, kubecfg := serviceOver(t, map[string]string{"prod": "key-2"})
	s.record("prod", "key-1", State{Identity: Identity{ServerUID: "uid-1"}})

	_, known := s.Get("prod")
	assert.False(t, known, "the answer is about credentials the context no longer names")

	kubecfg.keys["prod"] = "key-1"
	_, known = s.Get("prod")
	assert.True(t, known, "and readable again for the credentials it was learned under")
}
