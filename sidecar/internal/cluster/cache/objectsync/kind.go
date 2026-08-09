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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Kind identifies the collection one worker mirrors: where to list/watch (APIVersion +
// Resource) and how to key the rows (Kind).
type Kind struct {
	APIVersion string
	Kind       string
	Resource   string
	Namespaced bool
}

// scope is the kind_catalog spelling of Namespaced.
func (k Kind) scope() string {
	if k.Namespaced {
		return "Namespaced"
	}
	return "Cluster"
}

// group returns the api group of the kind's APIVersion ("" for the core group).
func (k Kind) group() string { return apiGroup(k.APIVersion) }

// isCRD reports whether the kind is a custom resource, recorded so the UI can group
// built-ins apart. Discovery doesn't say and asking costs a request per kind, so it's
// inferred from the api group (built-ins are core, a few legacy groups, or *.k8s.io).
// Wrong for a CRD installed under a k8s.io group — a display nuance, not correctness.
func (k Kind) isCRD() bool {
	g := k.group()
	if g == "" || strings.HasSuffix(g, ".k8s.io") {
		return false
	}
	switch g {
	case "apps", "batch", "autoscaling", "policy", "extensions":
		return false
	}
	return true
}

// GVR resolves the kind to the dynamic client's endpoint (core group as a bare "v1").
func (k Kind) GVR() (schema.GroupVersionResource, error) {
	if k.APIVersion == "" || k.Resource == "" {
		return schema.GroupVersionResource{}, fmt.Errorf("objectsync: incomplete kind %+v", k)
	}
	gv, err := schema.ParseGroupVersion(k.APIVersion)
	if err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("objectsync: parse apiVersion %q: %w", k.APIVersion, err)
	}
	return gv.WithResource(k.Resource), nil
}
