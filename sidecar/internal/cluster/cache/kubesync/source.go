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

package kubesync

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
)

// dynamicSource is the production Source: cluster-wide list/watch of one GVR over the
// dynamic client, so one worker mirrors every namespace (a cluster-scoped kind lists the
// same way — the dynamic client's unnamespaced form covers both).
//
// It lives here rather than in each specialization because "list/watch one GVR" is the
// only production source shape there is: the specializations differ in WHICH GVR, which
// is an argument, not a different implementation.
type dynamicSource struct {
	dyn dynamic.Interface
	// meta serves metadata-only lists — the cheap identity read the diff resync compares
	// against the cache before deciding which bodies to fetch.
	meta metadata.Interface
	gvr  schema.GroupVersionResource
}

var _ Source = (*dynamicSource)(nil)

// NewDynamicSource builds a Source over cfg for one GVR.
func NewDynamicSource(cfg *rest.Config, gvr schema.GroupVersionResource) (Source, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	meta, err := metadata.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &dynamicSource{dyn: dyn, meta: meta, gvr: gvr}, nil
}

func (s *dynamicSource) List(ctx context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
	ul, err := s.dyn.Resource(s.gvr).List(ctx, opts)
	if err != nil {
		return nil, "", "", err
	}
	out := make([]*unstructured.Unstructured, len(ul.Items))
	for i := range ul.Items {
		out[i] = &ul.Items[i]
	}
	return out, ul.GetContinue(), ul.GetResourceVersion(), nil
}

func (s *dynamicSource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return s.dyn.Resource(s.gvr).Watch(ctx, opts)
}

// ListMetadata lists this collection's identities only — no spec, no status. The API
// server serves it from the same store as a full LIST, but the response is a small
// fraction of the bytes, which is what makes the diff resync worth doing.
func (s *dynamicSource) ListMetadata(ctx context.Context, opts metav1.ListOptions) ([]ObjectMeta, string, string, error) {
	ml, err := s.meta.Resource(s.gvr).List(ctx, opts)
	if err != nil {
		return nil, "", "", err
	}
	out := make([]ObjectMeta, len(ml.Items))
	for i := range ml.Items {
		m := &ml.Items[i]
		out[i] = ObjectMeta{
			UID:             string(m.UID),
			Namespace:       m.Namespace,
			Name:            m.Name,
			ResourceVersion: m.ResourceVersion,
		}
	}
	return out, ml.GetContinue(), ml.GetResourceVersion(), nil
}

// Get fetches one full object. An empty namespace addresses a cluster-scoped kind.
func (s *dynamicSource) Get(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	// An empty namespace needs no branch: the dynamic client emits the namespaces/<ns>
	// URL segment only for a non-empty one, so this IS the cluster-scoped path.
	return s.dyn.Resource(s.gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}
