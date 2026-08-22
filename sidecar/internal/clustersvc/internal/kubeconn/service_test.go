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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// The shape the cluster service composes this into.
var _ lifecycle.StartCloser = (*Service)(nil)

// fakeKubeconfig resolves the contexts it holds and reports every other one departed, which is
// what the real service does.
type fakeKubeconfig struct {
	keys map[string]string
}

func (f *fakeKubeconfig) RESTConfig(contextName string) (*rest.Config, string, error) {
	key, ok := f.keys[contextName]
	if !ok {
		return nil, "", fmt.Errorf("%w: %q", kubeconfig.ErrContextNotFound, contextName)
	}
	return &rest.Config{Host: "https://" + contextName + ".example"}, key, nil
}

func TestStopAndCloseAreIdempotent(t *testing.T) {
	s := New(&fakeKubeconfig{keys: map[string]string{"a": "k1"}})

	stop, err := s.Start(context.Background())
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))
	require.NoError(t, stop(context.Background()))
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}
