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

package connections

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

func TestManagerGetMissReturnsNil(t *testing.T) {
	cm := NewManager()
	cfg, fp := cm.Get(domain.ClusterID(999999))
	assert.Nil(t, cfg)
	assert.Empty(t, fp)
}

func TestManagerSetThenGet(t *testing.T) {
	cm := NewManager()
	id := domain.ClusterID(1)
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}

	cm.Set(id, cfg, "fp-1")

	gotCfg, gotFP := cm.Get(id)
	assert.Equal(t, cfg, gotCfg)
	assert.Equal(t, "fp-1", gotFP)
}

func TestManagerDeleteRemovesEntry(t *testing.T) {
	cm := NewManager()
	id := domain.ClusterID(1)
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}

	cm.Set(id, cfg, "fp-1")
	cm.Delete(id)

	gotCfg, gotFP := cm.Get(id)
	assert.Nil(t, gotCfg)
	assert.Empty(t, gotFP, "a deleted entry must not leave its fingerprint behind")
}

func TestManagerDeleteMissingIsNoop(t *testing.T) {
	cm := NewManager()
	// Should not panic.
	cm.Delete(domain.ClusterID(999999))
}

func TestManagerSetOverwrites(t *testing.T) {
	cm := NewManager()
	id := domain.ClusterID(1)
	first := &rest.Config{Host: "https://first:6443"}
	second := &rest.Config{Host: "https://second:6443"}

	cm.Set(id, first, "fp-1")
	cm.Set(id, second, "fp-2")

	gotCfg, gotFP := cm.Get(id)
	assert.Equal(t, second, gotCfg)
	assert.Equal(t, "fp-2", gotFP, "the config and its fingerprint must move together")
}

func TestManagerConcurrentAccess(t *testing.T) {
	cm := NewManager()
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n * 3)
	for i := range n {
		id := domain.ClusterID(1)
		cfg := &rest.Config{Host: "https://host"}
		go func() { defer wg.Done(); cm.Set(id, cfg, "fp") }()
		go func() { defer wg.Done(); cm.Get(id) }() //nolint:errcheck // exercising the lock, not the value
		go func() { defer wg.Done(); cm.Delete(id) }()
		_ = i
	}
	wg.Wait()
}
