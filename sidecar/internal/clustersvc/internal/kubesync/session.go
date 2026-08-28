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
// of runs and streams under it.
package kubesync

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/supervisor"
)

// session is one tracked cache. Its two claims are the session's rather than each run's: a
// tracked cache has a file the moment it is armed, which is what Manager.WatchOpen readers
// are waiting on, and one lease is what everything under it dials over. It takes them in start
// and gives them back in close — both halves here, so neither can outlive the other.
type session struct {
	s       *Service
	cacheID int64
	params  Params

	// Held from start until close.
	lease kubeconn.Lease
	store *kubestore.Store

	// ctx bounds every run and every stream under this session; cancel ends them all.
	ctx    context.Context
	cancel context.CancelFunc

	// wakeLoops holds the session's two wake loops, which carry wakes rather than running a
	// body. runs holds everything that can still write through the claims: the runs in
	// flight, and the streams they started, which outlive them.
	wakeLoops sync.WaitGroup
	runs      sync.WaitGroup

	// Everything below is guarded by Service.mu, like the session map itself — one lock
	// covers a session, what is running under it, and their answers, so no read finds one
	// without the others.
	//
	// stopping closes runs to new registrations, so nothing joins the group after the wait
	// on it has begun.
	stopping bool
	// kindRuns holds the kind run in flight for each kind, and only while one is out. It is
	// what ForgetKind and a rename cancel: Remove reaches the schedule and the committed
	// stream, never a run already dispatched.
	kindRuns          map[kindID]*kindRun
	discoveryState    DiscoveryState
	hasDiscoveryState bool
	kindStates        map[kindID]KindState
	// relistNeeded holds the kinds whose cookie names a position the server has dropped. It
	// outlives the run that learned it: the 410 arrives on a run that establishes nothing, and
	// an intent that did not survive a later failure would send the next start back to the
	// same dead cookie. Cleared only when a cold list has landed.
	relistNeeded map[kindID]bool
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
		s:            s,
		cacheID:      cacheID,
		params:       p,
		ctx:          ctx,
		cancel:       cancel,
		kindRuns:     map[kindID]*kindRun{},
		kindStates:   map[kindID]KindState{},
		relistNeeded: map[kindID]bool{},
	}
}

// discoverySubject is this cache's name on the discovery supervisor.
func (sess *session) discoverySubject() string { return discoverySubject(sess.cacheID) }

// start takes the two claims, then arms the sweep and the two things that wake it. The
// supervisor owns the sweep's cadence and its backoff ladder, so there is no loop here — only
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
	sess.wakeLoops.Go(func() { sess.wakeOnConnectionChange(sess.ctx, connStateSub) })

	// Writes to this cache's own CustomResourceDefinition and APIService rows — the synced
	// objects, not the kind_catalog the sweep itself writes, so a sweep cannot wake itself.
	//
	// Promptness alone, so a store that will not subscribe costs a wake rather than the
	// sweep: a cache is armed below whether or not this attaches.
	catalogChangeSub, err := sess.store.Subscribe(
		kubestore.ObjectsKey(crdGVR.GroupVersion().String(), crdGVR.Resource),
		kubestore.ObjectsKey(apiServiceAPIVersion, apiServiceResource))
	if err == nil {
		sess.wakeLoops.Go(func() { sess.wakeDiscoverySweepOnCatalogChange(sess.ctx, catalogChangeSub) })
	}

	sess.s.discoverySupervisor.Add(sess.discoverySubject())
	return nil
}

// wakeOnConnectionChange is the session's one connection bridge, and it carries both directions
// neither the sweep nor a kind run can see for itself. A connection that arrived brings back
// whatever suspended for the want of one, which cannot wait for it because a run in flight holds
// a supervisor worker. A connection that stopped dialing makes a settled verdict wrong, and a
// settled sweep is scheduled rather than parked — so without this it would read Discovered until
// its interval came round.
//
// The pool's answer is ONE fact for every kind under the cache, so the guard below is applied
// once per session rather than per kind: a suspended cache of three hundred kinds is not polled
// per frame.
//
// Level-triggered against the facts rather than edge-triggered on the connection: WatchState
// keeps only the latest value per key, so a reader that falls behind skips the frame where
// the connection went away, and the edge that matters with it.
func (sess *session) wakeOnConnectionChange(ctx context.Context, connStateSub kubeconn.StateSubscription) {
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
			if sess.discoveryReason() != connectionReason(err, ReasonDiscoveryFailed) {
				sess.wakeAll()
			}
		case sess.parked():
			sess.wakeAll()
		}
	}
}

// wakeAll brings back everything under this cache: the sweep, and every kind, each of which
// suspends at the same gate for the same reason.
func (sess *session) wakeAll() {
	sess.s.discoverySupervisor.Wake(sess.discoverySubject(), discoveryProbes...)
	for _, k := range sess.trackedKinds() {
		sess.s.kindSupervisor.Wake(kindSubject(sess.cacheID, k), nameKindSync)
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
// APIService rows. Those two kinds are what change a catalog and the cache already syncs
// both, so a private watch here would be a second watch on the same collections over the
// same connection.
//
// The whole sweep, not the fan-out alone: a CRD for a group the cluster did not serve adds
// that group to /apis, so the list the fan-out reads has moved too — waking only the fan-out
// leaves the new kind unseen until the group list's own interval comes round.
//
// The loop it forms — discovery arms the kinds whose writes wake discovery — bottoms out
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
			sess.s.discoverySupervisor.Wake(sess.discoverySubject(), discoveryProbes...)
		}
	}
}

// sweepParked reports a sweep waiting on a wake: a suspended run schedules nothing, so its
// next attempt is zero.
func (s *Service) sweepParked(subject string) bool {
	snap, ok := s.discoverySupervisor.Read(subject)
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

// parked reports anything under this cache waiting on a wake. A suspended run schedules nothing,
// so its next attempt is zero.
//
// **Kinds are asked too, not just the sweep.** They suspend at the same gate and a connection
// that came back is the only thing that revives them, so a guard that read the sweep alone would
// leave every kind suspended whenever the sweep happened to have settled first.
func (sess *session) parked() bool {
	if sess.s.sweepParked(sess.discoverySubject()) {
		return true
	}
	for _, k := range sess.trackedKinds() {
		snap, ok := sess.s.kindSupervisor.Read(kindSubject(sess.cacheID, k))
		if ok && !snap.Attempts(nameKindSync).Scheduled() {
			return true
		}
	}
	return false
}

// announce wakes the cache with its verdict unmoved — the one publication a reason cannot
// carry. Two sweeps can both settle on Discovered with a CRD appearing between them, so a
// reason-only feed would leave the new kind unsynced until something unrelated moved.
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

// trackedKinds is what this session should be syncing: the kinds registered against its
// cache, snapshotted so the caller acts outside s.mu. The supervisor keeps no subject listing
// and needs none — this map is already exactly the set that was Added.
func (sess *session) trackedKinds() []kubestore.Kind {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	kinds := make([]kubestore.Kind, 0, len(sess.s.tracked[sess.cacheID]))
	for _, k := range sess.s.tracked[sess.cacheID] {
		kinds = append(kinds, k)
	}
	return kinds
}

// kindState is what this kind last committed, empty until it has.
func (sess *session) kindState(id kindID) (KindState, bool) {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	state, ok := sess.kindStates[id]
	return state, ok
}

// dropKindState forgets what a kind committed. **Called after the join, which is what makes it
// final** — not the tracked guard, which a rename does not provide: the kind stays tracked under
// its new singular, so commitKind refuses nothing the old generation reports on its way down.
// Dropped before that report, the entry comes back, and a withdrawn stream's Watching is served
// for a kind that has not listed a row.
func (sess *session) dropKindState(id kindID) {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	delete(sess.kindStates, id)
	delete(sess.relistNeeded, id)
}

// markRelist records that this kind's cookie names a position the server has dropped, and
// needsRelist reads it back. clearRelist is called only once a cold list has committed — the
// intent has to outlast every failure between learning it and acting on it, or a relist refused
// once would resume from the cookie it was replacing.
func (sess *session) markRelist(id kindID) {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	sess.relistNeeded[id] = true
}

func (sess *session) needsRelist(id kindID) bool {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	return sess.relistNeeded[id]
}

func (sess *session) clearRelist(id kindID) {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	delete(sess.relistNeeded, id)
}

// restart re-enters every kind off its cookie and re-runs the sweep. The sweep is a wake
// rather than a cancel: its runs are the supervisor's, and a wake is redelivered to one already
// in flight. A kind is a cancel of its stream — the exit is the wake, and a clean one carries
// no error, so the run it brings round re-establishes at once and holds its reason.
func (sess *session) restart() {
	sess.s.discoverySupervisor.Wake(sess.discoverySubject(), discoveryProbes...)
	for _, k := range sess.trackedKinds() {
		if stream := sess.committedStream(k); stream != nil {
			stream.cancel()
		}
		// A stream a run has committed but the supervisor has not applied yet is in no
		// snapshot, and a poke that missed it would leave that kind on the stream a sleeping
		// machine may already have killed.
		if stream := sess.pendingStream(idOf(k)); stream != nil {
			stream.cancel()
		}
	}
}

// committedStream is the stream the kind supervisor holds for one kind, or nil.
func (sess *session) committedStream(k kubestore.Kind) *kindStream {
	snap, ok := sess.s.kindSupervisor.Read(kindSubject(sess.cacheID, k))
	if !ok {
		return nil
	}
	// A kind armed but not yet established holds no stream, and its zero Observation carries
	// a nil handle either way.
	return supervisor.Get[*kindStream](snap, nameKindSync).Value
}

// pendingStream is the stream the run in flight for one kind has started, or nil.
func (sess *session) pendingStream(id kindID) *kindStream {
	sess.s.mu.Lock()
	defer sess.s.mu.Unlock()
	run := sess.kindRuns[id]
	if run == nil {
		return nil
	}
	return run.stream.Load()
}

// close ends every kind and the sweep, waits for them, and gives the two claims back — one
// call, because a claim released while a body could still write through it is a write into a
// file nothing holds open, and the join is the only thing standing between them.
//
// Dropping the subject stops the sweep being SCHEDULED, and a run in flight against it then
// commits nothing — but it neither cancels that run nor joins it, so the cancel and the wait
// below are what the release rests on.
func (sess *session) close() {
	sess.s.discoverySupervisor.Remove(sess.discoverySubject())
	// Each kind's Remove hands its stream back — cancel and join — so the goroutines are down
	// before the cancel below reaches the runs that started them.
	for _, k := range sess.trackedKinds() {
		sess.s.kindSupervisor.Remove(kindSubject(sess.cacheID, k))
	}
	sess.cancel()
	sess.wait()

	sess.store.Release()
	sess.lease.Release()
}

// wait joins the wake loops, every run in flight, and every stream a run started. It closes the
// session to new runs first, so nothing registers after the join has begun.
func (sess *session) wait() {
	sess.s.mu.Lock()
	sess.stopping = true
	sess.s.mu.Unlock()

	sess.wakeLoops.Wait()
	sess.runs.Wait()
}

// enterRun registers a run against its cache, so a teardown waits for it, and hands back the
// session it runs against. False for a cache nobody has armed and for one already stopping —
// both runs that must not write.
func (s *Service) enterRun(cacheID int64) (*session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enterRunLocked(cacheID)
}

func (s *Service) enterRunLocked(cacheID int64) (*session, bool) {
	sess, ok := s.sessions[cacheID]
	if !ok || sess.stopping {
		return nil, false
	}
	sess.runs.Add(1)
	return sess, true
}

func (sess *session) leaveRun() { sess.runs.Done() }

// kindRun is one kind reconciler run in flight: what cancels it, what says it is over, and the
// stream it started if it got that far.
type kindRun struct {
	id     kindID
	cancel context.CancelFunc
	done   chan struct{}
	// stream is what this run committed, set before the commit. The supervisor hands a refused
	// commit back only after the body returns, so a join that waited on the run alone could
	// return while the goroutine it started was still applying deltas.
	stream atomic.Pointer[kindStream]
}

// join waits for a cancelled run and stops whatever stream it started. A nil run — nothing
// was in flight — is nothing to wait for.
func (run *kindRun) join() {
	if run == nil {
		return
	}
	<-run.done
	if stream := run.stream.Load(); stream != nil {
		stream.stop()
	}
}

// enterKindRun admits one kind's run. **One critical section, and that is the point**: it
// checks the cache, reads the kind, and registers the run's cancel together. Split apart, a
// ForgetKind landing between the read and the registration would find no run to cancel, and
// the run would list rows for a kind nobody tracks — the relist landing behind a clear that
// the seam orders ForgetKind before ClearKind to rule out.
//
// The whole kubestore.Kind comes back, singular included: the subject names a kind by the pair
// the server guarantees unique, and the rows are keyed by a name no body can learn from a
// collection that lists empty.
func (s *Service) enterKindRun(cacheID int64, id kindID, cancel context.CancelFunc) (*session, kubestore.Kind, *kindRun, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k, ok := s.tracked[cacheID][id]
	if !ok {
		return nil, kubestore.Kind{}, nil, false
	}
	sess, ok := s.enterRunLocked(cacheID)
	if !ok {
		return nil, kubestore.Kind{}, nil, false
	}
	run := &kindRun{id: id, cancel: cancel, done: make(chan struct{})}
	sess.kindRuns[id] = run
	return sess, k, run, true
}

// leaveKindRun ends one kind run: whoever cancelled it is let go, and the registration goes
// unless a stream is standing behind it.
//
// **A run that started a stream keeps its entry past its own return.** The supervisor applies
// what a body committed only after the body returns, so between the two this handle is the only
// thing that can reach that stream — Remove finds no standing value to hand back yet. The next
// run for this kind replaces the entry, and it cannot start until the commit has landed, by
// which time Remove reaches the stream instead.
func (s *Service) leaveKindRun(sess *session, run *kindRun) {
	s.mu.Lock()
	if sess.kindRuns[run.id] == run && run.stream.Load() == nil {
		delete(sess.kindRuns, run.id)
	}
	s.mu.Unlock()

	close(run.done)
	sess.leaveRun()
}

// cancelKindRun cancels the run in flight for one kind and hands it back for the caller to
// join once armMu is released. Nil when none is out.
func (sess *session) cancelKindRun(id kindID) *kindRun {
	sess.s.mu.Lock()
	run := sess.kindRuns[id]
	sess.s.mu.Unlock()

	if run != nil {
		run.cancel()
	}
	return run
}
