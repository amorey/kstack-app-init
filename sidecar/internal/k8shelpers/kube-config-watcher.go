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
	"fmt"
	"log/slog"
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

	// Always watch the parent directory of each precedence path, never
	// the file itself. Two reasons:
	//
	//   1. Linux inotify detaches a file-level watch the moment the
	//      inode is unlinked or renamed (the editor / clientcmd
	//      atomic-replace pattern). Subsequent writes to the new inode
	//      go unobserved. Parent-dir watches don't have this problem —
	//      they keep firing across rename-over-write cycles.
	//   2. It uniformly handles "file exists" and "file absent at
	//      startup" — the same parent watch surfaces a later CREATE.
	//
	// inotify and ReadDirectoryChangesW deliver child events natively
	// when a directory is watched; macOS kqueue does it via fsnotify's
	// watchDirectoryFiles helper. The start() filter trims dir-level
	// noise down to just our precedence paths.
	watchedPaths := make(map[string]struct{}, len(loadingRules.GetLoadingPrecedence()))
	watchedDirs := make(map[string]struct{})
	for _, pathname := range loadingRules.GetLoadingPrecedence() {
		clean := filepath.Clean(pathname)
		watchedPaths[clean] = struct{}{}

		dir := filepath.Dir(clean)
		if _, already := watchedDirs[dir]; already {
			continue
		}
		if err := watcher.Add(dir); err != nil {
			// Parent dir doesn't exist (or is unwatchable). Skip; the
			// watcher stays usable, just blind to this path.
			slog.Info("kubeconfig parent dir unwatchable, skipping",
				"path", pathname, "parent", dir, "err", err)
			continue
		}
		watchedDirs[dir] = struct{}{}
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

// SetCurrentContext persists `name` as the kubeconfig current-context.
//
// It validates that the context exists in the loaded config (rejecting
// empty/unknown names before touching disk), then writes the change via
// clientcmd.ModifyConfig — not WriteToFile, which would flatten a
// multi-file kubeconfig into one file and lose each entry's
// locationOfOrigin. ModifyConfig writes the minimal current-context delta
// to the file that already defines it (or, if unset, the first file in
// precedence), matching `kubectl config use-context` semantics.
//
// The in-memory snapshot is updated and republished to subscribers
// immediately rather than relying on the debounced fsnotify echo, so the
// switch is delivered deterministically. The echo that follows reloads an
// identical config — an idempotent, harmless second publish.
func (w *KubeConfigWatcher) SetCurrentContext(name string) error {
	updated, err := w.writeCurrentContext(name)
	if err != nil {
		return err
	}

	// Publish after releasing the lock so a slow receiver can't stall the
	// fan-out while we hold w.mu (mirrors start()'s send-without-lock).
	w.tx.Send(updated) //nolint:errcheck

	return nil
}

// writeCurrentContext validates `name`, persists it as the current-context,
// and updates the in-memory snapshot — all under the write lock. It returns
// the new config for SetCurrentContext to publish once the lock is released.
func (w *KubeConfigWatcher) writeCurrentContext(name string) (*api.Config, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.kubeConfig == nil {
		return nil, fmt.Errorf("no kubeconfig loaded")
	}
	if name == "" {
		return nil, fmt.Errorf("context name is empty")
	}
	if _, ok := w.kubeConfig.Contexts[name]; !ok {
		return nil, fmt.Errorf("unknown context %q", name)
	}

	// ModifyConfig diffs against what's on disk and writes only the
	// current-context change to the right file. Pass a copy so a write
	// failure can't leave the in-memory CurrentContext mutated.
	updated := w.kubeConfig.DeepCopy()
	updated.CurrentContext = name

	pathOptions := clientcmd.NewDefaultPathOptions()
	pathOptions.LoadingRules = w.loadingRules
	if err := clientcmd.ModifyConfig(pathOptions, *updated, false); err != nil {
		return nil, err
	}

	w.kubeConfig = updated
	return updated, nil
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
