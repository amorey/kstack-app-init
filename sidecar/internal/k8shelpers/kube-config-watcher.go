// Copyright 2024 The Kubetail Authors
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

package k8shelpers

import (
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/amorey/gochan/watch"
	"github.com/fsnotify/fsnotify"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

const HOMEPATH_TILDE = "~"

// Subscription represents an active subscription that can be cancelled
type Subscription = *watch.Receiver[*api.Config]

// Represents KubeConfigWatcher
type KubeConfigWatcher struct {
	kubeConfig   *api.Config
	loadingRules *clientcmd.ClientConfigLoadingRules
	watcher      *fsnotify.Watcher
	hub          *watch.Hub[*api.Config]
	tx           *watch.Sender[*api.Config]
	// Canonicalized precedence paths. Used to filter fsnotify events down
	// to relevant files when we're watching parent directories (which
	// emit events for every child).
	watchedPaths map[string]struct{}
	mu           sync.RWMutex
}

// NewKubeConfigWatcher constructs a watcher that always returns a usable
// instance. Missing kubeconfig paths are skipped (logged, not fatal) so
// the sidecar can start on a fresh machine with no cluster wired up — the
// renderer's empty-state surfaces this. fsnotify-NewWatcher failures
// (kernel-level: ENOMEM, ulimit, /proc not mounted) are the only fatal
// case left, since without an inotify handle we can't watch anything.
func NewKubeConfigWatcher(kubeconfigPath string) (*KubeConfigWatcher, error) {
	// Initialize loading rules (outsources kubeconfig file/env handling to clientcmd library)
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath

	// Initialize watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	// Attach fsnotify to each precedence path. For paths whose file is
	// missing today, fall back to watching the parent directory so a
	// later CREATE delivers an event — the start() filter narrows those
	// down to just our files. Parent-dir adds are deduped (multiple
	// missing precedence files in the same dir share one watch).
	watchedPaths := make(map[string]struct{}, len(loadingRules.GetLoadingPrecedence()))
	watchedDirs := make(map[string]struct{})
	for _, pathname := range loadingRules.GetLoadingPrecedence() {
		clean := filepath.Clean(pathname)
		watchedPaths[clean] = struct{}{}

		if err := watcher.Add(pathname); err != nil {
			if !os.IsNotExist(err) {
				watcher.Close()
				return nil, err
			}
			// File missing — watch the parent so we observe the CREATE.
			dir := filepath.Dir(clean)
			if _, already := watchedDirs[dir]; already {
				continue
			}
			if dirErr := watcher.Add(dir); dirErr != nil {
				// Parent dir missing too — nothing to attach to. Skip;
				// the watcher stays usable, just blind to this file.
				slog.Info("kubeconfig path absent and parent unwatchable, skipping",
					"path", pathname, "parent", dir, "err", dirErr)
				continue
			}
			watchedDirs[dir] = struct{}{}
		}
	}

	// Load whatever clientcmd resolves from the precedence list. Returns
	// an empty *api.Config (not an error) when no file is found, which is
	// the state we want to seed: subscribers get a real pointer, field
	// resolvers iterate empty maps, the picker renders the empty state.
	cfg, err := loadingRules.Load()
	if err != nil {
		// Load only errors on a malformed file, not on absence. Treat as
		// non-fatal: log it and degrade to an empty config so the rest of
		// the sidecar surfaces stay up.
		slog.Warn("kubeconfig load failed, starting with empty config", "err", err)
		cfg = &api.Config{}
	}

	hub := watch.New[*api.Config](cfg)
	w := &KubeConfigWatcher{
		kubeConfig:   cfg,
		loadingRules: loadingRules,
		watcher:      watcher,
		hub:          hub,
		tx:           hub.Sender(),
		watchedPaths: watchedPaths,
	}

	// Start event listeners
	go w.start()

	return w, nil
}

// Get
func (w *KubeConfigWatcher) Get() *api.Config {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.kubeConfig == nil {
		return &api.Config{}
	}

	return w.kubeConfig
}

// Subscribe
func (w *KubeConfigWatcher) Subscribe() Subscription {
	return w.hub.Receiver()
}

// Close
func (w *KubeConfigWatcher) Close() {
	w.watcher.Close()
	w.tx.Close()
}

// Start
func (w *KubeConfigWatcher) start() {
	var debounceTimer *time.Timer
	var debounceDelay = 100 * time.Millisecond

	for {
		select {
		case err, ok := <-w.watcher.Errors:
			// Kill goroutine on watcher close
			if !ok {
				return
			}

			// Log error and keep listening
			slog.Error("kubeconfig watcher error", "err", err)
		case fsEv, ok := <-w.watcher.Events:
			// Kill goroutine on watcher close
			if !ok {
				return
			}

			// Ignore events for files we don't care about. When a parent
			// dir is watched (for absent precedence paths), fsnotify
			// delivers events for every child — drop noise here.
			if _, ok := w.watchedPaths[filepath.Clean(fsEv.Name)]; !ok {
				continue
			}

			// Handle fsnotify Create, Write, Remove events
			if fsEv.Has(fsnotify.Create) || fsEv.Has(fsnotify.Write) || fsEv.Has(fsnotify.Remove) {
				// Reset timer if it's already running
				if debounceTimer != nil {
					debounceTimer.Stop()
				}

				// Start a new timer
				debounceTimer = time.AfterFunc(debounceDelay, func() {
					// Reload config
					cfg, err := w.reloadConfig()
					if err != nil {
						slog.Error("kubeconfig reload failed", "err", err)
						return
					}

					// Publish event
					w.tx.Send(cfg) //nolint:errcheck
				})
			}
		}
	}
}

// Reload config
func (w *KubeConfigWatcher) reloadConfig() (*api.Config, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cfg, err := w.loadingRules.Load()
	if err != nil {
		return nil, err
	}
	w.kubeConfig = cfg

	return cfg, nil
}
