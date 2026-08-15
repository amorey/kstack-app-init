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

package clustersvc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Deterministic per (anchor, apiVersion, resource), which is what lets a discovery
// pass reconcile its children as a set with no per-child bookkeeping. Keyed on the
// plural, not the Kind: a CRD may reuse a built-in's plural under another group, so
// the group-version has to be in the name too.
func TestClusterCachedResourceName(t *testing.T) {
	assert.Equal(t, "cachedresource/3/apps/v1/deployments", ClusterCachedResourceName(3, "apps/v1", "deployments"))
	assert.NotEqual(t,
		ClusterCachedResourceName(3, "apps/v1", "deployments"),
		ClusterCachedResourceName(3, "example.com/v1", "deployments"),
		"the same plural under another group is a different kind")
	assert.NotEqual(t,
		ClusterCachedResourceName(3, "apps/v1", "deployments"),
		ClusterCachedResourceName(4, "apps/v1", "deployments"),
		"the same kind under another cache's anchor")
}
