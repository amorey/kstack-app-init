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

package engine

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
)

// liveSource is the production kubeSource for one GVR: full bodies + watch from
// the dynamic client, metadata-only lists from the metadata client. List/Watch
// are cluster-wide, so one driver mirrors a namespaced kind across all namespaces.
type liveSource struct {
	dyn  dynamic.Interface
	meta metadata.Interface
	gvr  schema.GroupVersionResource
}

func newLiveSource(dyn dynamic.Interface, meta metadata.Interface, e gvrEntry) *liveSource {
	return &liveSource{dyn: dyn, meta: meta, gvr: e.GVR}
}

var _ kubeSource = (*liveSource)(nil)

func (s *liveSource) List(ctx context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, error) {
	ul, err := s.dyn.Resource(s.gvr).List(ctx, opts)
	if err != nil {
		return nil, "", err
	}
	out := make([]*unstructured.Unstructured, len(ul.Items))
	for i := range ul.Items {
		out[i] = &ul.Items[i]
	}
	return out, ul.GetResourceVersion(), nil
}

func (s *liveSource) ListMetadata(ctx context.Context, opts metav1.ListOptions) ([]objMeta, string, error) {
	ml, err := s.meta.Resource(s.gvr).List(ctx, opts)
	if err != nil {
		return nil, "", err
	}
	out := make([]objMeta, len(ml.Items))
	for i := range ml.Items {
		m := &ml.Items[i]
		out[i] = objMeta{
			UID:             string(m.UID),
			Namespace:       m.Namespace,
			Name:            m.Name,
			ResourceVersion: m.ResourceVersion,
		}
	}
	return out, ml.GetResourceVersion(), nil
}

func (s *liveSource) Get(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	if namespace == "" {
		return s.dyn.Resource(s.gvr).Get(ctx, name, metav1.GetOptions{})
	}
	return s.dyn.Resource(s.gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (s *liveSource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return s.dyn.Resource(s.gvr).Watch(ctx, opts)
}
