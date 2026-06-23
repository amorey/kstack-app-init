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
	"sync"

	"k8s.io/client-go/rest"
)

// ConnectionManager holds the live REST config for each connected cluster.
// ClusterController writes to it on probe success/failure; ClusterCacheController
// and future agent callers read from it to obtain credentials without re-resolving
// the kubeconfig on every reconcile.
type ConnectionManager struct {
	mu      sync.RWMutex
	configs map[ClusterID]*rest.Config
}

// NewConnectionManager returns an empty ConnectionManager.
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		configs: make(map[ClusterID]*rest.Config),
	}
}

// Set stores (or replaces) the REST config for id.
func (m *ConnectionManager) Set(id ClusterID, cfg *rest.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configs[id] = cfg
}

// Get returns the REST config for id, or nil if none is stored.
func (m *ConnectionManager) Get(id ClusterID) *rest.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.configs[id]
}

// Delete removes the REST config for id. It is a no-op if id is not present.
func (m *ConnectionManager) Delete(id ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.configs, id)
}
