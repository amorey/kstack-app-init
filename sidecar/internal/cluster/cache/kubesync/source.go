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

// dynamicSource is the production Source: cluster-wide list/watch of one GVR, so one
// worker mirrors every namespace (the unnamespaced form covers cluster-scoped kinds too).
// The specializations differ only in WHICH GVR, which is an argument.
type dynamicSource struct {
	dyn dynamic.Interface
	// meta serves metadata-only lists — the diff resync's cheap identity read.
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

// ListMetadata lists identities only — same server-side store as a full LIST, a fraction
// of the bytes, which is what makes the diff resync worth doing.
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

// Get fetches one full object; an empty namespace needs no branch, since the dynamic
// client emits the namespaces/<ns> segment only for a non-empty one.
func (s *dynamicSource) Get(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	return s.dyn.Resource(s.gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}
