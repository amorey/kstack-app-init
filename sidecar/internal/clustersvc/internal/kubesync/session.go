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
// dials over. It takes them in start and gives them back in close — both halves here, so
// neither can outlive the other.
type session struct {
	s       *Service
	cacheID int64
	params  Params

	// Held from start until close.
	lease kubeconn.Lease
	store *kubestore.Store

	// ctx bounds every worker under this session; stop cancels it.
	ctx    context.Context
	cancel context.CancelFunc

	// wg holds the session's own goroutines — the two wake loops — which carry wakes rather
	// than running a body, so they are not workers. runs holds the sweeps in flight, which
	// are the engine's goroutines rather than this session's.
	wg   sync.WaitGroup
	runs sync.WaitGroup

	// Everything below is guarded by Service.mu, like the session map itself — one lock
	// covers a session, its workers, and their answers, so no read finds one without the
	// others.
	//
	// stopping closes runs to new registrations, so nothing joins the group after the wait
	// on it has begun.
	stopping          bool
	kindWorkers       map[kindID]*worker
	discoveryState    DiscoveryState
	hasDiscoveryState bool
	kindStates        map[kindID]KindState
	// sweepPartial and sweepMessage are the one thing a probe Result cannot carry: a run
	// that succeeded without answering for every group-version. Succeeded takes no reason,
	// and both neighbours are wrong — Fail would climb the backoff ladder over an aggregated
	// API that is down, and a plain success would read as Discovered.
	sweepPartial bool
	sweepMessage string
}

func newSession(s *Service, cacheID int64, p Params) *session {
	ctx, cancel := context.WithCancel(s.ctx)
	return &session{
		s:           s,
		cacheID:     cacheID,
		params:      p,
		ctx:         ctx,
		cancel:      cancel,
		kindWorkers: map[kindID]*worker{},
		kindStates:  map[kindID]KindState{},
	}
}

// subject is this cache's name on the probe engine.
func (sess *session) subject() string { return subjectOf(sess.cacheID) }

// start takes the two claims, then arms the sweep and the two things that wake it. The
// engine owns the sweep's cadence and its backoff ladder, so there is no loop here — only
// the wakes it cannot derive.
//
// A store that will not open arms nothing, and the caller retries on its next pass: there
// is nothing to unwind, because the lease is taken only once the file is open.
//
// Both subscriptions are taken before the goroutines that read them, never inside: a value
// published in the gap reaches nobody, and the sweep suspends rather than polling.
func (sess *session) start() error {
	store, err := sess.s.storeMgr.OpenOrCreate(sess.cacheID)
	if err != nil {
		return err
	}
	sess.store = store
	sess.lease = sess.s.connSvc.Acquire(sess.params.ContextName)

	// Every move in this lease's connection: one appearing or going away, and the identity
	// it answers as.
	connStateSub := sess.lease.WatchState()
	sess.wg.Go(func() { sess.wakeDiscoverySweepOnConnectionChange(sess.ctx, connStateSub) })

	// Writes to this cache's own CustomResourceDefinition and APIService rows — the mirrored
	// objects, not the kind_catalog the sweep itself writes, so a sweep cannot wake itself.
	//
	// Promptness alone, so a store that will not subscribe costs a wake rather than the
	// sweep: a cache is armed below whether or not this attaches.
	catalogChangeSub, err := sess.store.Subscribe(
		kubestore.ObjectsKey(crdGVR.GroupVersion().String(), crdGVR.Resource),
		kubestore.ObjectsKey(apiServiceAPIVersion, apiServiceResource))
	if err == nil {
		sess.wg.Go(func() { sess.wakeDiscoverySweepOnCatalogChange(sess.ctx, catalogChangeSub) })
	}

	sess.s.discoveryEngine.Add(sess.subject())
	return nil
}

// wakeDiscoverySweepOnConnectionChange carries both directions the sweep cannot see for
// itself. A connection that arrived brings back a sweep that suspended for the want of one,
// which cannot wait for it because a run in flight holds an engine worker. A connection that
// stopped dialing makes a settled verdict wrong, and a settled sweep is scheduled rather than
// parked — so without this it would read Discovered until its interval came round.
//
// Level-triggered against the facts rather than edge-triggered on the connection: WatchState
// keeps only the latest value per key, so a reader that falls behind skips the frame where
// the connection went away, and the edge that matters with it.
func (sess *session) wakeDiscoverySweepOnConnectionChange(ctx context.Context, connStateSub kubeconn.StateSubscription) {
	defer connStateSub.Close()
	for {
		if _, err := connStateSub.RecvContext(ctx); err != nil {
			return
		}
		switch _, err := sess.lease.ConnFor(ctx, sess.params.ServerUID); {
		case err != nil:
			// Only what the verdict does not already say. This feed publishes every pass, not
			// only the ones that changed something, so waking on each frame would make a
			// suspended sweep poll — which is the one thing suspending is for.
			if sess.discoveryReason() != connectionReason(err) {
				sess.s.discoveryEngine.Wake(sess.subject(), discoveryProbes...)
			}
		case sess.s.sweepParked(sess.subject()):
			sess.s.discoveryEngine.Wake(sess.subject(), discoveryProbes...)
		}
	}
}

// discoveryReason is the verdict this cache stands behind, empty until a run commits one.
func (sess *session) discoveryReason() Reason {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	if !sess.hasDiscoveryState {
		return ""
	}
	return Reason(sess.discoveryState.Reason)
}

// wakeDiscoverySweepOnCatalogChange rides the cache's own CustomResourceDefinition and
// APIService rows. Those two kinds are what change a catalog and the cache already mirrors
// both, so a private watch here would be a second watch on the same collections over the
// same connection.
//
// The whole sweep, not the fan-out alone: a CRD for a group the cluster did not serve adds
// that group to /apis, so the list the fan-out reads has moved too — waking only the fan-out
// leaves the new kind unseen until the group list's own interval comes round.
//
// The loop it forms — discovery starts the workers whose writes wake discovery — bottoms out
// on the interval, which is also the cold start. It can only ever be a wake: an api server
// upgrade changes the built-in kinds with no CRD or APIService write at all.
func (sess *session) wakeDiscoverySweepOnCatalogChange(ctx context.Context, catalogChangeSub kubestore.Subscription) {
	defer catalogChangeSub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-catalogChangeSub.Chan():
			if !ok {
				return
			}
			sess.s.discoveryEngine.Wake(sess.subject(), discoveryProbes...)
		}
	}
}

// sweepParked reports a sweep waiting on a wake: a suspended run schedules nothing, so its
// next attempt is zero.
func (s *Service) sweepParked(subject string) bool {
	snap, ok := s.discoveryEngine.Read(subject)
	if !ok {
		return false
	}
	for _, name := range discoveryProbes {
		if !snap.Attempts(name).Scheduled() {
			return true
		}
	}
	return false
}

// announce wakes the cache with its verdict unmoved — the one publication a reason cannot
// carry. Two sweeps can both settle on Discovered with a CRD appearing between them, so a
// reason-only feed would leave the new kind unmirrored until something unrelated moved.
func (sess *session) announce() {
	_ = sess.s.discoveryHub.Sender().Send(sess.cacheID, struct{}{})
}

// recordSweep and lastSweep carry what a Result cannot: whether the last fan-out answered
// for every group-version.
func (sess *session) recordSweep(partial bool, message string) {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	sess.sweepPartial, sess.sweepMessage = partial, message
}

func (sess *session) lastSweep() (bool, string) {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	return sess.sweepPartial, sess.sweepMessage
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
		sess.s.syncKindFn(ctx, kindRun{
			Kind:   k,
			Conn:   conn,
			Store:  sess.store,
			Commit: func(state KindState) { sess.s.commitKind(sess, k, state) },
		})
	})

	sess.s.mu.Lock()
	sess.kindWorkers[idOf(k)] = w
	sess.s.mu.Unlock()
}

// stopKind ends one kind's worker and waits for it — what makes ForgetKind synchronous.
func (sess *session) stopKind(id kindID) {
	sess.s.mu.Lock()
	w, ok := sess.kindWorkers[id]
	delete(sess.kindWorkers, id)
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

// restart re-enters every mirror off its cookie and re-runs the sweep. The sweep is a wake
// rather than a cancel: its runs are the engine's, and a wake is redelivered to one already
// in flight.
func (sess *session) restart() {
	sess.s.discoveryEngine.Wake(sess.subject(), discoveryProbes...)
	for _, w := range sess.workers() {
		w.restart()
	}
}

// close ends every worker and every sweep, waits for them, and gives the two claims back —
// one call, because a claim released while a body could still write through it is a write
// into a file nothing holds open, and the join is the only thing standing between them.
//
// Dropping the subject stops the sweep being SCHEDULED, and a run in flight against it then
// commits nothing — but it neither cancels that run nor joins it, so the cancel and the wait
// below are what the release rests on.
func (sess *session) close() {
	sess.s.discoveryEngine.Remove(sess.subject())
	sess.cancel()
	sess.wait()

	sess.store.Release()
	sess.lease.Release()
}

// wait joins the workers, the wake loops, and any sweep in flight. It closes the session to new
// sweeps first, so nothing registers after the join has begun.
func (sess *session) wait() {
	sess.s.mu.Lock()
	sess.stopping = true
	sess.s.mu.Unlock()

	for _, w := range sess.workers() {
		w.wait()
	}
	sess.wg.Wait()
	sess.runs.Wait()
}

// enterSweep registers a run against its cache, so a teardown waits for it, and hands back
// the session it runs against. False for a cache nobody has armed and for one already
// stopping — both runs that must not write.
func (s *Service) enterSweep(cacheID int64) (*session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, ok := s.sessions[cacheID]
	if !ok || sess.stopping {
		return nil, false
	}
	sess.runs.Add(1)
	return sess, true
}

func (sess *session) leaveSweep() { sess.runs.Done() }

func (sess *session) workers() []*worker {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	ws := make([]*worker, 0, len(sess.kindWorkers))
	for _, w := range sess.kindWorkers {
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
