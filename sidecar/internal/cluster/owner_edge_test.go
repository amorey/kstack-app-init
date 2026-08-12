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

	"github.com/amorey/beehive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// Every child kind's domain builder reads its parent id through this, so the
// fallback matters as much as the happy path: an object whose owner edge was not
// eager-loaded — or a hard delete, whose edge beehive has already collected — must
// read 0 rather than fail the frame. The client keys removal on the object's own id,
// so a 0 owner costs nothing there.
func TestOwnerObjectIDReadsTheEagerLoadedEdge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, coreCC, _ := newServiceTest(t)

	id := seedCluster(t, s, "alpha")
	cacheID := seedActiveCache(t, s, coreCC, id, "uid-alpha")

	// Loaded edge: the cache's owner is its cluster.
	obj, err := s.cacheClient.Get(ctx, beehive.ObjectID(cacheID), beehive.LoadOwner())
	require.NoError(t, err)
	assert.Equal(t, domain.ObjectID(id), ownerObjectID(obj))

	// Same object read WITHOUT the load: no edge to read, so 0 — never a panic and
	// never a wrong id.
	bare, err := s.cacheClient.Get(ctx, beehive.ObjectID(cacheID))
	require.NoError(t, err)
	assert.Equal(t, domain.ObjectID(0), ownerObjectID(bare))
}
