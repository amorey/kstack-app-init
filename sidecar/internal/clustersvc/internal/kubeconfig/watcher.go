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

// Package kubeconfig watches the user's kubeconfig and publishes each reload. A
// leaf: it speaks native kubeconfig vocabulary (*api.Config) and knows nothing
// about cluster records.
package kubeconfig

import (
	"context"
	"sync"
	"time"

	"github.com/amorey/gochan/watch"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
)

// Subscription is current-on-subscribe and delivers each reload after that.
// Cancel it through its own Close.
type Subscription = *watch.Receiver[*api.Config]

// defaultPollInterval paces the reload. Sized for a file a human edits by hand:
// the cost of a stale kube-context for a moment is nil, and the cost of parsing
// the file on a tight loop is not.
const defaultPollInterval = 2 * time.Second

// Watcher reloads the kubeconfig on a timer and publishes it only when it
// changed, so an unchanged file writes nothing downstream.
//
// Polling is the whole mechanism, not a fallback: filesystem notifications are
// unreliable for exactly this file (editors and clientcmd both replace the inode
// rather than write in place, detaching a file-level watch). A notification layer
// added later only wakes the poll early — subscribers cannot tell which woke it,
// so it stays a change inside this file.
type Watcher struct {
	loadingRules *clientcmd.ClientConfigLoadingRules
	// interval paces the reload; tests shrink it before Start rather than outwait it.
	interval time.Duration

	hub *watch.Hub[*api.Config]

	mu      sync.RWMutex
	current *api.Config

	wg sync.WaitGroup
}

// New returns a watcher for kubeconfigPath, or the standard precedence chain when
// it is empty. Nothing is read until Start: a subscriber attaching before then sees
// an empty config rather than a nil one.
func New(kubeconfigPath string) *Watcher {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath

	current := &api.Config{}
	return &Watcher{
		loadingRules: loadingRules,
		interval:     defaultPollInterval,
		hub:          watch.New(current),
		current:      current,
	}
}

// Get returns the most recently loaded config; never nil.
func (w *Watcher) Get() *api.Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.current
}

// Subscribe returns a handle carrying the current config, then each reload.
func (w *Watcher) Subscribe() Subscription {
	return w.hub.Receiver()
}

// Start reloads once, then launches the poll loop and returns the func that ends it.
// The first reload is synchronous so that whoever subscribes after Start sees real
// state: on the empty seed an importer would reconcile a config known to hold
// nothing. Nothing here can fail.
func (w *Watcher) Start(context.Context) (func(context.Context) error, error) {
	w.poll()

	stop := make(chan struct{})
	w.wg.Go(func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				w.poll()
			}
		}
	})

	return func(ctx context.Context) error {
		close(stop)
		return drain.WithContext(ctx, w.wg.Wait)
	}, nil
}

// Close ends every outstanding Subscription. Separate from the stop func, and after
// it: subscribers must keep reading until the poll loop is joined, or a reload in
// flight is lost rather than delivered.
func (w *Watcher) Close() error {
	w.hub.Close()
	return nil
}

// poll reloads the kubeconfig and publishes it if it differs from the last
// snapshot. Not implemented.
func (w *Watcher) poll() {}
