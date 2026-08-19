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

// White-box, unlike this package's other tests: start order is the parts slice, and
// nothing on the public surface reports it.
package app

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// The cluster reconciles read the kubeconfig on their first pass and defer while it
// reports unread, which is a state only this ordering keeps them out of: Start reads
// synchronously, so a service started after it always observes a read. Reversed, every
// record would observe its context absent and orphan itself — a mass write, not a
// pause — and the guards in clustersvc are the only thing that would stand between the
// reordering and that.
func TestKubeconfigStartsBeforeTheClusterService(t *testing.T) {
	a, err := New(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, a.Close()) })

	assert.Less(t, partIndex(t, a, "kubeconfig service"), partIndex(t, a, "cluster service"))
}

// partIndex returns where the named part sits in start order.
func partIndex(t *testing.T, a *App, name string) int {
	t.Helper()
	i := slices.IndexFunc(a.parts, func(p lifecycle.Part) bool { return p.Name == name })
	require.GreaterOrEqual(t, i, 0, "no part named %q", name)
	return i
}
