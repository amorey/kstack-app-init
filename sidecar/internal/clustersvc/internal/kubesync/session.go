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

// One armed cache: its connection claim, its store claim, the identity gate, and the set
// of workers under it.
package kubesync

import (
	"context"
	"sync"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
)

// session is one tracked cache. Its two claims are the session's rather than each
// worker's: a tracked cache has a file the moment it is armed, which is what
// Manager.WatchOpen readers are waiting on, and one lease is what every worker under it
// dials over.
type session struct {
	s       *Service
	cacheID int64
	params  Params
	lease   kubeconn.Lease
	store   *kubestore.Store

	// ctx bounds every worker under this session; stop cancels it.
	ctx    context.Context
	cancel context.CancelFunc

	// Everything below is guarded by Service.mu, like the session map itself — one lock
	// covers a session, its workers, and their answers, so no read finds one without the
	// others.
	discovery         *worker
	kinds             map[kindID]*worker
	discoveryState    DiscoveryState
	hasDiscoveryState bool
	kindStates        map[kindID]KindState
}

func newSession(s *Service, cacheID int64, p Params, lease kubeconn.Lease, store *kubestore.Store) *session {
	ctx, cancel := context.WithCancel(s.ctx)
	return &session{
		s:          s,
		cacheID:    cacheID,
		params:     p,
		lease:      lease,
		store:      store,
		ctx:        ctx,
		cancel:     cancel,
		kinds:      map[kindID]*worker{},
		kindStates: map[kindID]KindState{},
	}
}

// startDiscovery arms the sweep. It is handed the lease rather than a connection: a sweep
// runs on the probe engine, where blocking for one would hold an engine worker.
func (sess *session) startDiscovery() {
	run := discoveryRun{
		Params:   sess.params,
		Lease:    sess.lease,
		Store:    sess.store,
		Commit:   func(st DiscoveryState) { sess.s.commitDiscovery(sess, st) },
		Announce: func() { _ = sess.s.discoveryHub.Sender().Send(sess.cacheID, struct{}{}) },
	}
	w := newWorker(sess.ctx, func(ctx context.Context) { sess.s.discoveryBody(ctx, run) })

	sess.s.mu.Lock()
	sess.discovery = w
	sess.s.mu.Unlock()
}

// startKind arms one kind's mirror behind the identity gate. Nothing syncs into a cache
// whose connection does not vouch for its ServerUID, and a kind worker holds its own
// goroutine, so it waits for one rather than reporting and suspending.
func (sess *session) startKind(k kubestore.Kind) {
	w := newWorker(sess.ctx, func(ctx context.Context) {
		conn, err := kubeconn.AwaitConnFor(ctx, sess.lease, sess.params.ServerUID)
		if err != nil {
			return
		}
		sess.s.kindBody(ctx, kindRun{
			Kind:   k,
			Conn:   conn,
			Store:  sess.store,
			Commit: func(st KindState) { sess.s.commitKind(sess, k, st) },
		})
	})

	sess.s.mu.Lock()
	sess.kinds[idOf(k)] = w
	sess.s.mu.Unlock()
}

// stopKind ends one kind's worker and waits for it — what makes ForgetKind synchronous.
func (sess *session) stopKind(id kindID) {
	sess.s.mu.Lock()
	w, ok := sess.kinds[id]
	delete(sess.kinds, id)
	sess.s.mu.Unlock()
	if ok {
		w.stop()
	}
}

// dropKindState forgets what a kind's worker committed. **Only after stopKind**, which is
// what makes it final: a verdict belongs to the worker that answered it, and one still in
// flight would write the entry back — leaving a stopped worker's Watching to be served
// for a kind that is cold-listing, or for one nobody tracks any more.
func (sess *session) dropKindState(id kindID) {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	delete(sess.kindStates, id)
}

// restart re-enters every body under this session off its cookie.
func (sess *session) restart() {
	for _, w := range sess.workers() {
		w.restart()
	}
}

// stop ends every worker and waits for it, which is what a caller forgetting this cache
// needs: nothing can still write through the store afterwards.
func (sess *session) stop() {
	sess.cancel()
	sess.wait()
}

// wait joins the workers without ending them — the drain half, for a stop that cancelled
// the whole process's context instead.
func (sess *session) wait() {
	for _, w := range sess.workers() {
		w.wait()
	}
}

// release gives the claims back. Only safe once the workers are joined.
func (sess *session) release() {
	sess.store.Release()
	sess.lease.Release()
}

func (sess *session) workers() []*worker {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	ws := make([]*worker, 0, len(sess.kinds)+1)
	if sess.discovery != nil {
		ws = append(ws, sess.discovery)
	}
	for _, w := range sess.kinds {
		ws = append(ws, w)
	}
	return ws
}

// worker runs one body, re-entering it whenever a run ends while the worker is still
// armed. A restart is a cancel of the run in flight and nothing more, which is what lets
// a resume poke reach every kind without re-arming anything.
type worker struct {
	// cancel ends the worker: the loop exits once the current run returns.
	cancel context.CancelFunc
	done   chan struct{}

	mu sync.Mutex
	// cancelRun ends only the run in flight. Replaced per run, so a restart never cancels
	// the run that replaced the one it meant.
	cancelRun context.CancelFunc
}

func newWorker(parent context.Context, body func(context.Context)) *worker {
	ctx, cancel := context.WithCancel(parent)
	w := &worker{cancel: cancel, done: make(chan struct{})}

	go func() {
		defer close(w.done)
		for ctx.Err() == nil {
			runCtx, cancelRun := context.WithCancel(ctx)
			w.mu.Lock()
			w.cancelRun = cancelRun
			w.mu.Unlock()
			body(runCtx)
			cancelRun()
		}
	}()
	return w
}

// restart ends the run in flight; the loop enters the body again.
func (w *worker) restart() {
	w.mu.Lock()
	cancelRun := w.cancelRun
	w.mu.Unlock()
	if cancelRun != nil {
		cancelRun()
	}
}

// stop ends the worker and waits for the body to return.
func (w *worker) stop() {
	w.cancel()
	<-w.done
}

// wait joins a worker whose context something above already cancelled.
func (w *worker) wait() { <-w.done }
