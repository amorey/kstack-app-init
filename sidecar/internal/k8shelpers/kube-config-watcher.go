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

// HOMEPATH_TILDE is the unexpanded leading "~" in a kubeconfig path.
const HOMEPATH_TILDE = "~"

// KubeConfigSubscription delivers each new *api.Config; cancel via its own Close.
type KubeConfigSubscription = *watch.Receiver[*api.Config]

// KubeConfigWatcher loads the user's kubeconfig and watches its precedence
// paths for changes, publishing each reloaded *api.Config to subscribers.
type KubeConfigWatcher struct {
	current      *api.Config
	loadingRules *clientcmd.ClientConfigLoadingRules
	watcher      *fsnotify.Watcher
	hub          *watch.Hub[*api.Config]
	tx           *watch.Sender[*api.Config]
	// Canonicalized precedence paths, used to filter the parent-directory watches'
	// events down to the files we care about.
	watchedPaths map[string]struct{}
	mu           sync.RWMutex
	// wg tracks the event-loop goroutine, so Close can join it.
	wg sync.WaitGroup
}

// NewKubeConfigWatcher always returns a usable instance: missing kubeconfig paths are
// logged and skipped, so the sidecar starts on a machine with no cluster wired up. Only a
// kernel-level fsnotify failure is fatal — without an inotify handle nothing is watchable.
func NewKubeConfigWatcher(kubeconfigPath string) (*KubeConfigWatcher, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	// Watch each precedence path's PARENT DIR, never the file: inotify detaches a
	// file-level watch the moment the inode is replaced (the atomic-replace pattern
	// every editor and clientcmd uses), and a parent watch also surfaces a later
	// CREATE for a file absent at startup. eventLoop trims the dir-level noise.
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
			// The watcher stays usable, just blind to this path.
			slog.Info("kubeconfig parent dir unwatchable, skipping",
				"path", pathname, "parent", dir, "err", err)
			continue
		}
		watchedDirs[dir] = struct{}{}
	}

	// clientcmd returns an empty *api.Config, not an error, when no file is found —
	// the right seed, since subscribers get a real pointer.
	cfg, err := loadingRules.Load()
	if err != nil {
		// Load errors only on a malformed file; degrade so the sidecar stays up.
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

// Get returns the most recently loaded kubeconfig; never nil.
func (w *KubeConfigWatcher) Get() *api.Config {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.current == nil {
		return &api.Config{}
	}

	return w.current
}

// Subscribe returns a current-on-subscribe handle, then each reload.
func (w *KubeConfigWatcher) Subscribe() KubeConfigSubscription {
	return w.hub.Receiver()
}

// Start launches the watcher's event-loop goroutine.
func (w *KubeConfigWatcher) Start() {
	w.wg.Go(w.eventLoop)
}

// Close stops the fsnotify watcher (ending the event loop), joins it, and closes the hub,
// ending all subscriptions.
func (w *KubeConfigWatcher) Close() error {
	err := w.watcher.Close()
	w.wg.Wait()
	w.tx.Close()
	return err
}

// eventLoop filters fsnotify events to the watched precedence paths, debounces bursts,
// and republishes the reloaded config. Returns when the watcher is closed.
func (w *KubeConfigWatcher) eventLoop() {
	var debounceTimer *time.Timer
	var debounceDelay = 100 * time.Millisecond

	for {
		select {
		case err, ok := <-w.watcher.Errors:
			if !ok { // watcher closed
				return
			}
			slog.Error("kubeconfig watcher error", "err", err)
		case fsEv, ok := <-w.watcher.Events:
			if !ok { // watcher closed
				return
			}

			// A parent dir delivers events for every child.
			if _, ok := w.watchedPaths[filepath.Clean(fsEv.Name)]; !ok {
				continue
			}

			if fsEv.Has(fsnotify.Create) || fsEv.Has(fsnotify.Write) || fsEv.Has(fsnotify.Remove) {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.AfterFunc(debounceDelay, func() {
					cfg, err := w.reloadConfig()
					if err != nil {
						slog.Error("kubeconfig reload failed", "err", err)
						return
					}
					w.tx.Send(cfg) //nolint:errcheck
				})
			}
		}
	}
}

// reloadConfig re-resolves the kubeconfig and stores it as current.
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
