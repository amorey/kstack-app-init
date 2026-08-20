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

// Nothing is remembered between asks, which is what makes a context re-pointed at another
// server described correctly by the next one without anything having to notice the edit.
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
