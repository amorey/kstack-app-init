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

package cluster_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster"
)

func TestConnectionManagerGetMissReturnsNil(t *testing.T) {
	cm := cluster.NewConnectionManager()
	got := cm.Get(cluster.ClusterID(999999))
	assert.Nil(t, got)
}

func TestConnectionManagerSetThenGet(t *testing.T) {
	cm := cluster.NewConnectionManager()
	id := cluster.ClusterID(1)
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}

	cm.Set(id, cfg)

	got := cm.Get(id)
	assert.Equal(t, cfg, got)
}

func TestConnectionManagerDeleteRemovesEntry(t *testing.T) {
	cm := cluster.NewConnectionManager()
	id := cluster.ClusterID(1)
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}

	cm.Set(id, cfg)
	cm.Delete(id)

	got := cm.Get(id)
	assert.Nil(t, got)
}

func TestConnectionManagerDeleteMissingIsNoop(t *testing.T) {
	cm := cluster.NewConnectionManager()
	// Should not panic.
	cm.Delete(cluster.ClusterID(999999))
}

func TestConnectionManagerSetOverwrites(t *testing.T) {
	cm := cluster.NewConnectionManager()
	id := cluster.ClusterID(1)
	first := &rest.Config{Host: "https://first:6443"}
	second := &rest.Config{Host: "https://second:6443"}

	cm.Set(id, first)
	cm.Set(id, second)

	got := cm.Get(id)
	assert.Equal(t, second, got)
}

func TestConnectionManagerConcurrentAccess(t *testing.T) {
	cm := cluster.NewConnectionManager()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n * 3)
	for i := range n {
		id := cluster.ClusterID(1)
		cfg := &rest.Config{Host: "https://host"}
		go func() { defer wg.Done(); cm.Set(id, cfg) }()
		go func() { defer wg.Done(); cm.Get(id) }()
		go func() { defer wg.Done(); cm.Delete(id) }()
		_ = i
	}
	wg.Wait()
}
