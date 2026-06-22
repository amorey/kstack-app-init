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

package cachesync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
)

// End-to-end through the real fake clients: liveSource must decode a metadata
// list into objMeta and a dynamic GET into *unstructured, and the metadata-diff
// resync must then refresh exactly the changed object's row in SQLite. This pins
// the liveSource decoding the hand-rolled fakeSource can't (it bypasses the real
// client object shapes).
func TestLiveSourceMetadataDiffEndToEnd(t *testing.T) {
	ctx := context.Background()
	cdb := migratedCDB(t)
	w := cdb.Writer()
	// Cache already holds Pod "a" at RV 1 (so we're on the metadata-diff path).
	_, err := w.Exec(`INSERT INTO objects (uid, api_version, kind, name, namespace, resource_version, created_at, updated_at, raw_json)
		VALUES ('uid-a','v1','Pod','a','default','1',0,0,'{}')`)
	require.NoError(t, err)

	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	gvk := podGVK()

	// Dynamic client serves the full body for the GET (Pod "a" now at RV 2).
	scheme := runtime.NewScheme()
	podA := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"uid": "uid-a", "name": "a", "namespace": "default", "resourceVersion": "2",
		},
		"spec": map[string]any{"nodeName": "node-1"},
	}}
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{gvr: "PodList"}, podA)

	// Metadata client serves the metadata list (Pod "a" at RV 2).
	metaScheme := metadatafake.NewTestScheme()
	require.NoError(t, metav1.AddMetaToScheme(metaScheme))
	metaA := &metav1.PartialObjectMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{UID: "uid-a", Name: "a", Namespace: "default", ResourceVersion: "2"},
	}
	metaClient := metadatafake.NewSimpleMetadataClient(metaScheme, metaA)

	src := newLiveSource(dynClient, metaClient, gvrEntry{GVR: gvr, GVK: gvk, Namespaced: true})
	store := newObjectsStore(ctx, "c1", gvk, w, cdb)
	d := newKindDriver(src, store, gvk, "")

	_, err = d.fullResync(ctx)
	require.NoError(t, err)

	// The changed object's row is refreshed to RV 2, and the full body landed
	// (nodeName from the GET, proving the dynamic decode, not just metadata).
	var rv, host string
	require.NoError(t, w.QueryRow(`SELECT resource_version, COALESCE(host,'') FROM objects WHERE uid='uid-a'`).Scan(&rv, &host))
	require.Equal(t, "2", rv)
	require.Equal(t, "node-1", host)
}
