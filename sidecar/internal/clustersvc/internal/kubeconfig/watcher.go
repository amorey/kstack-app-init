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
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/amorey/gochan/watch"
	"github.com/fsnotify/fsnotify"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
)

// Subscription is current-on-subscribe and delivers each reload after that.
// Cancel it through its own Close.
type Subscription = *watch.Receiver[*api.Config]

// defaultPollInterval is the backstop, not the latency budget: notifications carry
// the ordinary case, so this only has to cover what they cannot see — a directory
// that became watchable after startup, a dropped event, a notifier that could not be
// created at all. Half an hour of staleness in those cases costs nothing; a tighter
// tick would parse the file all day for nobody.
const defaultPollInterval = 30 * time.Minute

// defaultSettle is the quiet period an edit goes through before it is read. Sized
// well above the gap a writer leaves between truncating a file and filling it
// (microseconds) and well below what a person notices.
const defaultSettle = 50 * time.Millisecond

// Watcher reloads the kubeconfig and publishes it only when it changed, so an
// unchanged file writes nothing downstream.
//
// Polling is what makes it correct; notifications only make it prompt. Filesystem
// events cannot be relied on for this file — an editor saves by replacing the inode,
// the notifier may not be available at all — so the tick is the floor under every one
// of those, and losing notifications costs latency alone. Subscribers cannot tell
// which source woke a reload, which keeps the whole arrangement inside this file.
//
// A resume is the third input. Timers run on the monotonic clock, which does not
// advance while the host is suspended, so waking can leave the tick most of a period
// still to run — and the events it would have covered are exactly the ones a
// suspended process is most likely to have dropped.
//
// The writers differ, and both shapes matter here: an editor replaces the inode,
// which is why the watch is on the directory; clientcmd (so `kubectl config
// use-context`) rewrites IN PLACE, truncating first, which is why an event settles
// before it is acted on.
type Watcher struct {
	loadingRules *clientcmd.ClientConfigLoadingRules
	// interval paces the reload; tests shrink it before Start rather than outwait it.
	interval time.Duration
	// settle is how long events must go quiet before a reload. It exists because a
	// truncated file is not a parse error — a zero-length kubeconfig loads as a valid
	// config with no contexts, and publishing that reads downstream as "every cluster
	// is gone". Waiting out the writer's own gap is what keeps that off the wire.
	settle time.Duration
	// pokeSvc wakes the loop on a resume. Nil is ordinary — the poll covers what it
	// would have caught, a period later.
	pokeSvc *poke.Service

	hub *watch.Hub[*api.Config]

	mu      sync.RWMutex
	current *api.Config
	// read records that a poll has been attempted. It separates "there is no
	// kubeconfig here" from "the watcher has not gotten to it yet" — both are the
	// empty config, so the value alone cannot say which. Set even when the load
	// failed: an unreadable kubeconfig is an answer, and the last good config (here,
	// the empty seed) is what stands.
	read bool

	wg sync.WaitGroup
}

// New returns a watcher for kubeconfigPath, or the standard precedence chain when
// it is empty. Nothing is read until Start: a subscriber attaching before then sees
// an empty config rather than a nil one. pokeSvc may be nil.
func New(kubeconfigPath string, pokeSvc *poke.Service) *Watcher {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath

	current := &api.Config{}
	return &Watcher{
		loadingRules: loadingRules,
		interval:     defaultPollInterval,
		settle:       defaultSettle,
		pokeSvc:      pokeSvc,
		hub:          watch.New(current),
		current:      current,
	}
}

// Get returns the most recently loaded config — never nil — and whether the watcher
// has read yet. Before the first read the config is the empty seed, which a caller
// must not observe: it would report every context absent.
func (w *Watcher) Get() (*api.Config, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.current, w.read
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
	// Watches before the first read, so the two overlap instead of leaving a gap: a
	// change after this is queued as an event, one before it lands in the read below,
	// and one in between arrives twice — which publishes once, since the second
	// reload finds nothing changed. Reading first would drop anything written in
	// that window until the backstop tick, half an hour later.
	paths := w.resolvePaths()
	fsw := newNotifier(paths)

	// Subscribed before the first read for the same reason the watch is: a poke in the
	// gap would otherwise wake nothing.
	var pokes <-chan poke.Signal
	cancelPokes := func() {}
	if w.pokeSvc != nil {
		pokes, cancelPokes = w.pokeSvc.Subscribe()
	}

	w.poll()

	stop := make(chan struct{})
	w.wg.Go(func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		defer cancelPokes()
		if fsw != nil {
			defer fsw.Close()
		}

		// events and errs stay nil when there is no notification layer, and a receive
		// on a nil channel blocks forever — so the loop degrades to the ticker alone
		// with no branch of its own.
		var events chan fsnotify.Event
		var errs chan error
		if fsw != nil {
			events, errs = fsw.Events, fsw.Errors
		}

		// Armed by an event or a poke, drained by the reload it triggers.
		settle := time.NewTimer(w.settle)
		settle.Stop()
		defer settle.Stop()

		// Re-resolving before each reload is what follows a re-pointed symlink onto
		// its new directory. Only this goroutine touches paths.
		reload := func() {
			paths = w.resolvePaths()
			if fsw != nil {
				syncDirs(fsw, paths)
			}
			w.poll()
		}

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				reload()
			case ev := <-events:
				// Deferred rather than acted on: a writer emits several events for one
				// save, and the first of them can arrive with the file still truncated.
				if _, ok := paths[filepath.Clean(ev.Name)]; ok {
					settle.Reset(w.settle)
				}
			case <-settle.C:
				reload()
			case _, ok := <-pokes:
				if !ok {
					// The bus closed ahead of us; the ticker still covers everything.
					pokes = nil
					continue
				}
				// Through the settle timer rather than straight to reload: a poke is
				// correlated with host events, so it can land inside a writer's
				// truncate-then-rewrite gap the same way an fsnotify event can.
				settle.Reset(w.settle)
			case err := <-errs:
				slog.Debug("kubeconfig watch error", "err", err)
			}
		}
	})

	// Idempotent, like every other stop func the composition calls: startAll/stopAll
	// treat them uniformly, so a retry or a double drain anywhere above must not turn
	// shutdown into a close of a closed channel.
	var once sync.Once
	return func(ctx context.Context) error {
		once.Do(func() { close(stop) })
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

// newNotifier returns a notifier watching the directories behind paths, or nil when
// the kernel would not give us one at all.
//
// Kept even when nothing is watchable yet — a machine with no ~/.kube has no
// directory to watch until the user first runs kubectl, and each reload retries.
// Discarding it there would mean discovering the config on a tick and then staying
// blind to every edit after it.
//
// nil is an ordinary outcome, not a failure: the poll is what makes the watcher
// correct, so losing notifications costs latency and nothing else.
func newNotifier(paths watchedPaths) *fsnotify.Watcher {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		slog.Info("kubeconfig notifications unavailable, polling only", "err", err)
		return nil
	}
	syncDirs(fsw, paths)
	return fsw
}

// watchedPaths is every file the loop reacts to: each precedence path, plus the file
// it resolves to when it is a symlink.
type watchedPaths map[string]struct{}

// resolvePaths reads the precedence chain and follows any symlink in it. A
// dotfiles-managed kubeconfig is a link whose target lives elsewhere, and an edit
// there touches nothing in the link's own directory — so without the target the
// notification layer never sees the routine case (`kubectl config use-context`
// writes straight through the link).
//
// Recomputed per reload rather than once, so re-pointing the link is picked up: the
// replacement is itself an event on the link, which is already watched.
func (w *Watcher) resolvePaths() watchedPaths {
	paths := watchedPaths{}
	for _, pathname := range w.loadingRules.GetLoadingPrecedence() {
		clean := filepath.Clean(pathname)
		paths[clean] = struct{}{}
		if target, ok := linkTarget(clean); ok {
			paths[target] = struct{}{}
		}
	}
	return paths
}

// linkTarget resolves pathname's symlink, falling back to the raw link contents when
// the target does not exist. The fallback is what covers a dangling link — a
// half-finished dotfiles checkout, a profile not yet materialized — where the
// target APPEARING is the event worth seeing and EvalSymlinks cannot name it.
func linkTarget(pathname string) (string, bool) {
	if target, err := filepath.EvalSymlinks(pathname); err == nil {
		return target, true
	}
	target, err := os.Readlink(pathname)
	if err != nil {
		return "", false
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(pathname), target)
	}
	return filepath.Clean(target), true
}

// syncDirs makes fsw watch exactly the directories holding paths. Directories rather
// than the files: an editor saves by replacing the inode, and a file-level watch
// follows the discarded one.
//
// Runs on every reload, which is what picks up a directory absent at startup and
// follows a symlink re-pointed at another one. Dropping the departed directories
// matters as much as adding: one left behind wakes the loop on every write in it for
// the life of the process, and the kqueue backend holds an fd per file it contains.
//
// A directory that cannot be watched is logged at debug and skipped: at this cadence
// anything louder would repeat for as long as it stays missing, and the poll already
// covers it.
func syncDirs(fsw *fsnotify.Watcher, paths watchedPaths) {
	want := map[string]struct{}{}
	for pathname := range paths {
		want[filepath.Dir(pathname)] = struct{}{}
	}

	for _, dir := range fsw.WatchList() {
		if _, keep := want[dir]; !keep {
			// Errors on a watch the kernel already dropped along with its directory,
			// which is the outcome being asked for.
			_ = fsw.Remove(dir)
		}
	}
	for dir := range want {
		if err := fsw.Add(dir); err != nil {
			slog.Debug("kubeconfig dir unwatchable, polling only for it", "dir", dir, "err", err)
		}
	}
}

// poll reloads the kubeconfig and publishes it if it differs from the last snapshot.
// markRead records the read a failed load still made.
func (w *Watcher) markRead() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.read = true
}

func (w *Watcher) poll() {
	// clientcmd returns an empty config rather than an error when no file is found,
	// which is the right reading: a machine with no kubeconfig tracks no clusters.
	cfg, err := w.loadingRules.Load()
	if err != nil {
		// Debug, not warn: a hand-edited file is unparseable in the middle of an edit
		// as a matter of course, and the last good config carries on serving. But
		// silence would leave a permanently broken kubeconfig looking like a watcher
		// that simply stopped noticing.
		slog.Debug("kubeconfig load failed, keeping the last good config", "err", err)
		w.markRead()
		return
	}

	// Compared whole rather than by a projection: the contexts drive the importer,
	// and the cluster and user entries behind them resolve credentials, so every
	// field reaches someone.
	w.mu.Lock()
	w.read = true
	unchanged := reflect.DeepEqual(w.current, cfg)
	if !unchanged {
		w.current = cfg
	}
	w.mu.Unlock()

	if unchanged {
		return
	}
	w.hub.Sender().Send(cfg)
}
