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

// The watch that makes a sweep prompt: what it listens to, and when it wakes the sweep
// rather than resuming quietly. It never commits — only Run does.
package kubecatalog

import (
	"context"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
)

// The two collections that change what a cluster serves. Separate streams because they are
// separate resources with separate versions: one aging out says nothing about the other.
var (
	crdGVR = schema.GroupVersionResource{
		Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
	}
	apiServiceGVR = schema.GroupVersionResource{
		Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices",
	}

	collections = [...]schema.GroupVersionResource{crdGVR, apiServiceGVR}
)

// reopenDelay is what a stream waits before reopening. Small: a reopen is the routine end of a
// server-side timeout, and the resume means the wait costs latency rather than events.
const reopenDelay = time.Second

// watcher is one subject's standing watch: a stream per collection, and the one cancel that
// ends them both. It never commits — it only wakes the sweep, which reads current state.
type watcher struct {
	// conn is what the streams stand over, kept so the sweep can tell its own connection from
	// the one this was built on: credentials replaced under an unchanged server UID leave the
	// sweep succeeding over the new connection while these streams hold the retired one.
	conn   *kubeconn.Connection
	cancel context.CancelFunc
	done   sync.WaitGroup

	// ended closes when a stream returns, however it got there. A watcher whose streams are
	// gone wakes nobody, so it has to read as absent rather than as one still standing.
	ended   chan struct{}
	endOnce sync.Once

	// opened closes once every stream has attempted its first open, whatever came of it. It is
	// what awaitOpen waits on.
	opened     chan struct{}
	firstOpens sync.WaitGroup

	// reopenDelay paces a stream that reopens. A parameter rather than the constant read
	// directly, so a test picks its own timescale.
	reopenDelay time.Duration
}

func newWatcher(conn *kubeconn.Connection, cancel context.CancelFunc, streams int, reopenDelay time.Duration) *watcher {
	w := &watcher{
		conn:        conn,
		cancel:      cancel,
		ended:       make(chan struct{}),
		opened:      make(chan struct{}),
		reopenDelay: reopenDelay,
	}
	w.firstOpens.Add(streams)
	go func() {
		w.firstOpens.Wait()
		close(w.opened)
	}()
	return w
}

// startWatcher stands both streams up over conn. The context is the caller's to derive from
// something longer-lived than a sweep: streams opened on a pass's context die when the pass
// returns, which would re-establish a dead watcher once per sweep forever.
func startWatcher(ctx context.Context, conn *kubeconn.Connection, open opener, reopenDelay time.Duration, wake func()) *watcher {
	ctx, cancel := context.WithCancel(ctx)
	w := newWatcher(conn, cancel, len(collections), reopenDelay)

	for _, gvr := range collections {
		w.done.Add(1)
		go func() {
			defer w.done.Done()
			w.stream(ctx, gvr, open, wake)
		}()
	}
	return w
}

// awaitOpen blocks until every stream has attempted its first open, or ctx ends.
//
// **The sweep must not read discovery before its watches are open.** A watch starts from the
// server's current state, so one that opens after the read emits nothing for a change made in
// between — a change that is in neither answer, and surfaces only at the next poll. Waiting
// here is what makes the two reads overlap instead of abut.
//
// ctx bounds the wait and never the streams, which live on the watcher's own context: a wait
// cut short by a slow open costs this pass the overlap and nothing else.
func (w *watcher) awaitOpen(ctx context.Context) {
	select {
	case <-w.opened:
	case <-ctx.Done():
	}
}

// stop ends both streams and waits for them, so a stopped watcher has nothing still running
// against a connection the caller is about to stop trusting. Idempotent.
func (w *watcher) stop() {
	w.cancel()
	w.done.Wait()
}

// spent reports whether a stream has returned. **Either one is enough**: the collection it
// watched is no longer watched, and what replaces it is a whole watcher rather than half of
// one — re-opening the survivor's collection costs a stream, and leaving it uncovered costs
// the promptness the watch exists for.
func (w *watcher) spent() bool {
	select {
	case <-w.ended:
		return true
	default:
		return false
	}
}

func (w *watcher) end() { w.endOnce.Do(func() { close(w.ended) }) }

// opener opens one collection's watch, resuming from resumeFrom when it is not empty.
type opener func(ctx context.Context, conn *kubeconn.Connection, gvr schema.GroupVersionResource, resumeFrom string) (watch.Interface, error)

// stream watches one collection until it cannot resume, waking the sweep on every change.
// It returns when ctx ends or the stream cannot be resumed.
//
// **A clean end resumes; it does not wake.** Servers close watches routinely, and without a
// version to reopen from, a quiet timeout is indistinguishable from a stream that dropped
// events — so every end would have to be swept, at several times the cost of the poll this
// accelerates.
//
// **Only an end that proves a gap wakes.** An end the watcher merely cannot continue from is
// silent, and re-establishing waits for the next sweep. Waking on one closes a loop, because
// the sweep's answer to a dead watch is to stand another one up: a refused watch — RBAC on a
// cluster-scoped collection, which is refused identically every time — would spend a full
// discovery pass per refusal, committing nothing that would make it visible.
func (w *watcher) stream(ctx context.Context, gvr schema.GroupVersionResource, open opener, wake func()) {
	// The version of the last event seen, change or bookmark. Empty until one lands, which is
	// the one clean end that cannot be resumed.
	var resumeFrom string
	firstOpen := true

	for {
		stream, err := open(ctx, w.conn, gvr, resumeFrom)
		if firstOpen {
			// Whatever came of it: a refused watch has no promptness to offer, and a sweep
			// held behind a handshake that will never complete is worse than a blind one.
			w.firstOpens.Done()
			firstOpen = false
		}
		if err != nil {
			w.end()
			return
		}

		next, end := consume(ctx, stream, resumeFrom, wake)
		stream.Stop()
		if end == endGap {
			w.gap(wake)
			return
		}
		if end == endStopped || next == "" {
			w.end()
			return
		}
		resumeFrom = next

		// Paced, because nothing else paces this: a proxy that hangs up as fast as it accepts
		// would otherwise be reopened as fast as it can close. Nothing is missed in the wait —
		// the reopen resumes from where the stream left off.
		select {
		case <-ctx.Done():
			w.end()
			return
		case <-time.After(w.reopenDelay):
		}
	}
}

// gap ends a stream that missed events: the watcher is spent, then the sweep is woken for the
// re-list that is the Kubernetes answer to a gap of unknown size. **That order is the
// contract** — the sweep the wake runs reads the watcher to decide whether to replace it, and
// one that still read as standing would leave the cluster on the poll for good.
//
// The wake cannot loop, because what the sweep stands up starts from the collection's version
// as it is now: a server refuses a version for being too old, so the replacement has to run
// long enough for its own start to age before it can end this way again.
func (w *watcher) gap(wake func()) {
	w.end()
	wake()
}

// streamEnd is how one stream ended, which is what decides whether the sweep is owed a run.
type streamEnd int

const (
	// endStopped: ctx ended. A teardown, not a gap.
	endStopped streamEnd = iota
	// endClosed: the server hung up, which servers do routinely. Resumable when a version
	// was seen over the stream, and otherwise merely over.
	endClosed
	// endGap: the server refused to serve from where we were, so events were missed and
	// only a sweep can recover them.
	endGap
)

// consume reads one stream to its end, waking on each change. It returns the version to resume
// from — empty when none was seen — beside how the stream ended.
func consume(ctx context.Context, stream watch.Interface, resumeFrom string, wake func()) (string, streamEnd) {
	for {
		select {
		case <-ctx.Done():
			return "", endStopped
		case ev, ok := <-stream.ResultChan():
			if !ok {
				return resumeFrom, endClosed
			}
			if ev.Type == watch.Error {
				return "", errorEnd(ev.Object)
			}
			if rv := versionOf(ev); rv != "" {
				resumeFrom = rv
			}
			if ev.Type == watch.Bookmark {
				continue
			}
			wake()
		}
	}
}

// errorEnd classifies the error the server ended a stream with. **Only a refusal of the version
// we asked from is a gap** — that one says events were missed, and the sweep is the only re-list
// there is. Anything else is the server's own trouble, says nothing about what we did or did not
// see, and repeats: waking over it would sweep, re-establish, and be refused again, at a full
// discovery pass per turn paced by nothing but how fast the server can fail.
func errorEnd(obj runtime.Object) streamEnd {
	err := apierrors.FromObject(obj)
	if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
		return endGap
	}
	return endClosed
}

// versionOf is the resource version an event carries, or empty for one that carries none.
func versionOf(ev watch.Event) string {
	obj, err := meta.Accessor(ev.Object)
	if err != nil {
		return ""
	}
	return obj.GetResourceVersion()
}

// openCollectionWatch is the production opener: one collection's watch over the connection's
// dynamic client, resuming from resumeFrom, or from the collection's current version when there
// is nothing to resume from.
//
// Bookmarks are asked for because they are what keeps a quiet stream resumable — without them
// a server-side timeout with nothing to report would have no version to reopen from, and every
// timeout would cost a sweep.
func openCollectionWatch(ctx context.Context, conn *kubeconn.Connection, gvr schema.GroupVersionResource, resumeFrom string) (watch.Interface, error) {
	if resumeFrom == "" {
		start, err := collectionVersion(ctx, conn, gvr)
		if err != nil {
			return nil, err
		}
		resumeFrom = start
	}
	return conn.Dynamic.Resource(gvr).Watch(ctx, metav1.ListOptions{
		AllowWatchBookmarks: true,
		ResourceVersion:     resumeFrom,
	})
}

// collectionVersion is where a fresh watch starts.
//
// **A watch given no version replays**: the server streams a synthetic Added for every object
// that already exists. Each one reads as a change here, so every establishment — startup, gap
// recovery, a rebuilt connection — would wake the sweep that is about to read the same state,
// and buy a redundant discovery pass for it.
//
// Limit 1, because only the collection's version is wanted; whatever object comes back with it
// is dropped.
func collectionVersion(ctx context.Context, conn *kubeconn.Connection, gvr schema.GroupVersionResource) (string, error) {
	list, err := conn.Dynamic.Resource(gvr).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", err
	}
	return list.GetResourceVersion(), nil
}
