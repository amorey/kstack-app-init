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

// Package connections holds each connected cluster's live credentials, and the
// fingerprint that decides when they have rotated. It is a leaf shared by both layers
// above it rather than a controller's private state: the core controller writes to the
// Manager, the sync controllers read it for credentials, and the cluster boundary reads
// it to answer GetConnection.
package connections

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"sync"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// Manager holds the live REST config for each connected cluster, alongside
// the config's fingerprint. ClusterCoreController writes to it on probe success/failure;
// the sync controllers and future agent callers read from it to obtain credentials
// without re-resolving the kubeconfig on every reconcile.
type Manager struct {
	mu      sync.RWMutex
	configs map[domain.ClusterID]connection
}

// connection is one cluster's credentials plus their fingerprint. The fingerprint is
// stored, not recomputed by readers: only the core controller sees the kubeconfig's raw
// proxy-url (clientcmd compiles it into an unhashable Proxy func), so a reader
// recomputing it would silently miss a proxy change.
type connection struct {
	cfg         *rest.Config
	fingerprint string
}

// NewManager returns an empty Manager.
func NewManager() *Manager {
	return &Manager{
		configs: make(map[domain.ClusterID]connection),
	}
}

// Set stores (or replaces) the REST config for id and the fingerprint identifying it.
func (m *Manager) Set(id domain.ClusterID, cfg *rest.Config, fingerprint string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configs[id] = connection{cfg: cfg, fingerprint: fingerprint}
}

// Get returns the stored config and fingerprint for id, or (nil, "") if none. One value
// under one read: separate reads could pair an OLD config with a NEW fingerprint, and a
// sync started that way looks "unchanged" to every later reconcile and never restarts.
func (m *Manager) Get(id domain.ClusterID) (*rest.Config, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c := m.configs[id]
	return c.cfg, c.fingerprint
}

// Delete removes the REST config for id. It is a no-op if id is not present.
func (m *Manager) Delete(id domain.ClusterID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.configs, id)
}

// Fingerprint hashes a rest.Config's connection/auth fields; a changed fingerprint
// is the credential-rotation trigger that restarts a cluster's sync. Hashes the *static*
// exec/auth-provider config — runtime token minting is the transport's job, but editing
// how tokens are obtained must invalidate it. proxyURL is passed raw (ContextProxyURL)
// because clientcmd compiles it into an unhashable rest.Config.Proxy func.
func Fingerprint(cfg *rest.Config, proxyURL string) string {
	if cfg == nil {
		return ""
	}
	h := sha256.New()
	// NUL-separate every field so boundaries can't be aliased by concatenation.
	write := func(s string) { h.Write([]byte(s)); h.Write([]byte{0}) }
	writeBytes := func(b []byte) { h.Write(b); h.Write([]byte{0}) }

	write(proxyURL)

	t := cfg.TLSClientConfig
	for _, s := range []string{
		cfg.Host, cfg.APIPath, cfg.Username, cfg.Password,
		cfg.BearerToken, cfg.BearerTokenFile,
		t.ServerName, t.CAFile, t.CertFile, t.KeyFile,
		strconv.FormatBool(t.Insecure),
	} {
		write(s)
	}
	writeBytes(t.CAData)
	writeBytes(t.CertData)
	writeBytes(t.KeyData)

	// Impersonation.
	im := cfg.Impersonate
	write(im.UserName)
	write(im.UID)
	for _, g := range im.Groups {
		write(g)
	}
	for _, k := range sortedKeys(im.Extra) {
		write(k)
		for _, v := range im.Extra[k] {
			write(v)
		}
	}

	// Auth-provider plugin (name + static config).
	if ap := cfg.AuthProvider; ap != nil {
		write(ap.Name)
		for _, k := range sortedKeys(ap.Config) {
			write(k)
			write(ap.Config[k])
		}
	}

	// Exec credential plugin (command/args/env/apiVersion).
	if ep := cfg.ExecProvider; ep != nil {
		write(ep.Command)
		write(ep.APIVersion)
		for _, a := range ep.Args {
			write(a)
		}
		for _, e := range ep.Env {
			write(e.Name)
			write(e.Value)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ContextProxyURL returns the proxy-url of the cluster a kubeconfig context points at,
// or "" if the context, its cluster, or the field is absent. The sync layer folds it into
// Fingerprint because clientcmd compiles it into rest.Config.Proxy, an opaque func
// the fingerprint can't otherwise see.
func ContextProxyURL(cfg *api.Config, ctxName string) string {
	ctx, ok := cfg.Contexts[ctxName]
	if !ok || ctx == nil {
		return ""
	}
	cluster, ok := cfg.Clusters[ctx.Cluster]
	if !ok || cluster == nil {
		return ""
	}
	return cluster.ProxyURL
}

// sortedKeys returns a map's keys in deterministic order, so hashing a map doesn't depend
// on Go's randomized iteration order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
