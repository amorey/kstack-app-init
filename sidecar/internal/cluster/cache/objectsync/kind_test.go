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

package objectsync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestKindGVR covers the one genuinely error-prone conversion here: discovery hands back
// an apiVersion, the dynamic client wants a GroupVersionResource, and the core group is
// spelled as a bare version with no slash.
func TestKindGVR(t *testing.T) {
	tests := []struct {
		apiVersion string
		resource   string
		want       schema.GroupVersionResource
	}{
		{"apps/v1", "deployments", schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}},
		{"v1", "pods", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}},
		{"gateway.networking.k8s.io/v1beta1", "gateways",
			schema.GroupVersionResource{Group: "gateway.networking.k8s.io", Version: "v1beta1", Resource: "gateways"}},
	}
	for _, tc := range tests {
		got, err := Kind{APIVersion: tc.apiVersion, Resource: tc.resource}.GVR()
		require.NoError(t, err)
		assert.Equal(t, tc.want, got)
	}
}

// TestKindGVRRejectsIncomplete pins that a kind we can't address is refused at
// construction rather than producing a worker that lists the wrong endpoint.
func TestKindGVRRejectsIncomplete(t *testing.T) {
	for _, k := range []Kind{
		{APIVersion: "", Resource: "pods"},
		{APIVersion: "v1", Resource: ""},
		{APIVersion: "a/b/c", Resource: "pods"},
	} {
		_, err := k.GVR()
		assert.Error(t, err, "%+v must not resolve to an endpoint", k)
	}
}

// TestKindIsCRD covers the api-group heuristic the kind_catalog's is_crd flag comes from.
func TestKindIsCRD(t *testing.T) {
	for _, apiVersion := range []string{"v1", "apps/v1", "batch/v1", "rbac.authorization.k8s.io/v1"} {
		assert.False(t, Kind{APIVersion: apiVersion}.isCRD(), "%s is a built-in group", apiVersion)
	}
	for _, apiVersion := range []string{"argoproj.io/v1alpha1", "cert-manager.io/v1"} {
		assert.True(t, Kind{APIVersion: apiVersion}.isCRD(), "%s is an operator's own group", apiVersion)
	}
}
