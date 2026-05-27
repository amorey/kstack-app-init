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
	"os"
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
	mu           sync.RWMutex
}

// Creates new KubeConfigWatcher instance
func NewKubeConfigWatcher(kubeconfigPath string) (*KubeConfigWatcher, error) {
	// Initialize loading rules (outsources kubeconfig file/env handling to clientcmd library)
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath

	// Initialize watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	// Watch kubeconfig paths
	for _, pathname := range loadingRules.GetLoadingPrecedence() {
		err = watcher.Add(pathname)
		if err != nil {
			watcher.Close()
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("kubeconfig file not found at '%s'.\n\nPlease ensure the file exists or use the '--kubeconfig' flag to specify a custom path.\nIf you are running inside a cluster, use the '--in-cluster' flag", pathname)
			}
			return nil, err
		}
	}

	hub := watch.New[*api.Config](nil)

	// Initialize kube-config-watcher instance
	w := &KubeConfigWatcher{
		loadingRules: loadingRules,
		watcher:      watcher,
		hub:          hub,
		tx:           hub.Sender(),
	}

	// Initialize config
	w.reloadConfig() //nolint:errcheck

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
