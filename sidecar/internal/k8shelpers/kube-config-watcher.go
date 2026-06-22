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

package k8shelpers

import (
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/amorey/gochan/watch"
	"github.com/fsnotify/fsnotify"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

// HOMEPATH_TILDE is the leading "~" that denotes the user's home directory in
// a kubeconfig path before expansion.
const HOMEPATH_TILDE = "~"

// KubeConfigSubscription is a handle to an active subscription on a
// KubeConfigWatcher, delivering each new *api.Config and cancellable via the
// receiver's own close.
type KubeConfigSubscription = *watch.Receiver[*api.Config]

// KubeConfigWatcher loads the user's kubeconfig and watches its precedence
// paths for changes, publishing each reloaded *api.Config to subscribers.
type KubeConfigWatcher struct {
	current      *api.Config
	loadingRules *clientcmd.ClientConfigLoadingRules
	watcher      *fsnotify.Watcher
	hub          *watch.Hub[*api.Config]
	tx           *watch.Sender[*api.Config]
	// Canonicalized precedence paths. Used to filter fsnotify events down
	// to relevant files when we're watching parent directories (which
	// emit events for every child).
	watchedPaths map[string]struct{}
	mu           sync.RWMutex
	// wg tracks the event-loop goroutine Start launches, so Close can join it.
	wg sync.WaitGroup
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
	// watchDirectoryFiles helper. The eventLoop filter trims dir-level
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

	hub := watch.New(cfg)
	w := &KubeConfigWatcher{
		current:      cfg,
		loadingRules: loadingRules,
		watcher:      watcher,
		hub:          hub,
		tx:           hub.Sender(),
		watchedPaths: watchedPaths,
	}

	return w, nil
}

// Get returns the most recently loaded kubeconfig, or an empty *api.Config if
// none has been loaded yet (never nil).
func (w *KubeConfigWatcher) Get() *api.Config {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.current == nil {
		return &api.Config{}
	}

	return w.current
}

// Subscribe registers a new subscriber and returns its handle. The subscriber
// receives the current config immediately, then each reload thereafter.
func (w *KubeConfigWatcher) Subscribe() KubeConfigSubscription {
	return w.hub.Receiver()
}

// Start launches the watcher's event-loop goroutine.
func (w *KubeConfigWatcher) Start() {
	w.wg.Go(w.eventLoop)
}

// Close stops the fsnotify watcher (which ends the event loop), waits for the
// loop to exit, and tears down the publish hub, ending all subscriptions.
func (w *KubeConfigWatcher) Close() error {
	err := w.watcher.Close()
	w.wg.Wait()
	w.tx.Close()
	return err
}

// eventLoop drains fsnotify events, filters them down to the watched
// precedence paths, debounces bursts, and reloads + republishes the config on
// each settled change. It returns when the fsnotify watcher is closed.
func (w *KubeConfigWatcher) eventLoop() {
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

// reloadConfig re-resolves the kubeconfig from the loading rules and stores it
// as the current config, returning the freshly loaded value.
func (w *KubeConfigWatcher) reloadConfig() (*api.Config, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cfg, err := w.loadingRules.Load()
	if err != nil {
		return nil, err
	}
	w.current = cfg

	return cfg, nil
}
