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

// Package kubesync mirrors each tracked kind into its cache's on-disk store — one
// worker per (cache, GVR), the machinery behind every ClusterCachedResource record.
//
// Subjects are opaque ids the caller supplies (clustersvc uses the record's beehive
// name, so the change signal doubles as the requeue), each bound at Track to the
// params a worker syncs with. Arming is the caller's policy, not interest — Track and
// Forget mirror the record's own state, so nothing a reader does can re-arm a kind
// the user paused. A worker is a standing push stream, not a periodic pass, which is
// why this runs its own goroutines rather than the probe engine.
//
// Track of a tracked id is a no-op while its params hold, and replaces the worker
// when they move; RestartAll restarts every worker in place, from the held params,
// keeping the last observations — which is what a resume poke needs, since nothing
// else would restart a worker whose watch died silently under a sleeping machine.
// → docs/adr/2026-08-26-sync-workers-not-probes.md.
package kubesync

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
)

// connService is the pool the workers borrow connections from: a lease per tracked
// subject — refcounted in the pool beside every other holder's, so releasing one
// never stops the cluster being probed.
type connService interface {
	Acquire(contextName string) kubeconn.Lease
}

// storeManager is the cache directory as a worker reaches it: one claim per cache,
// held for the run and given back when it ends. Leaf-to-leaf, the way the kubeconn
// import is — a worker writes native rows, and knows nothing of the records above.
type storeManager interface {
	OpenOrCreate(cacheID int64) (*kubestore.Store, error)
}

// Subscription reports the ids whose news changed — the verdict, never a count tick
// or timestamp — for the trigger that requeues each one's record. A keyed,
// coalescing bus, so a fleet syncing at once neither loses an id behind a busier one
// nor overflows a buffer. The value carries nothing; the key is the news.
type Subscription = *conflate.Receiver[string, struct{}]

// The Reason vocabulary an Observation carries — this leaf's own words, which the
// controller maps onto the record's condition reasons.
const (
	// ReasonNoConnection: the worker is suspended because nothing has reached the
	// server; the pool's wake resumes it.
	ReasonNoConnection = "NoConnection"
	// ReasonIdentityMismatch: the context's connection does not answer as the server
	// this cache mirrors; the worker is suspended rather than syncing another
	// cluster's objects into it.
	ReasonIdentityMismatch = "IdentityMismatch"
	// ReasonSyncing: the worker is building or catching up — listing, or watching
	// but not yet caught up.
	ReasonSyncing = "Syncing"
	// ReasonWatching: caught up and streaming deltas, proven live.
	ReasonWatching = "Watching"
	// ReasonStale: caught up, but the watch stopped proving itself alive past the
	// staleness threshold — the cache may be behind.
	ReasonStale = "Stale"
	// ReasonSyncFailed: the worker's run failed and is retrying with backoff.
	ReasonSyncFailed = "SyncFailed"
)

// Params is what one worker syncs: which kind, over which context, into which
// cache's store — and the identity that context must answer as, because a context
// re-pointed at another cluster must not sync that one's objects into this cache.
type Params struct {
	// CacheID names the store the worker writes, and is the key ForgetCache stops
	// by. The subject id cannot carry it: the record's name embeds the catalog's
	// object id, not the cache's.
	CacheID     int64
	ContextName string
	ServerUID   string
	APIVersion  string
	// Kind is the singular the rows are keyed by. The worker cannot wait to learn it
	// from a body: a collection that emptied while nothing was watching lists nothing,
	// and the relist's sweep would then match no rows, leaving every stale one behind.
	Kind     string
	Resource string
}

// Observation is one worker's standing answer: the verdict and the freshness facts
// the health fold reads. Reason is the news — the signal fires only when it moves.
type Observation struct {
	Reason  string
	Message string
	// Resumed reports that the run started from an existing cookie rather than
	// building the kind from nothing. It rides the commit that moves Reason, since a
	// flip under an unchanged Reason wakes nobody — the fold reads it to tell a first
	// sync from a resync.
	Resumed bool
	// ObjectCount is the kind's cached rows as of the last write.
	ObjectCount int
	// LastUpdateAt is when data last arrived; LastLiveAt the last proof the watch is
	// live (a delta or a bookmark). Zero until the worker has one.
	LastUpdateAt time.Time
	LastLiveAt   time.Time
}

// syncFunc is one worker's whole run: sync p over lease, into store, until ctx ends,
// publishing through commit (last call wins). The seam the tests substitute.
type syncFunc func(ctx context.Context, p Params, lease kubeconn.Lease, store *kubestore.Store, commit func(Observation))

// SubjectObservation is one tracked subject as the health fold reads it: which kind,
// into which cache, and the answer its worker stands behind.
type SubjectObservation struct {
	ID          string
	Params      Params
	Observation Observation
	Known       bool
}

// Service runs the workers over the tracked ids.
type Service struct {
	conns  connService
	stores storeManager
	sync   syncFunc
	// signalHub names the ids whose news changed, fed by each worker's commit.
	signalHub *conflate.Hub[string, struct{}]

	// openBase and openMax pace a worker's retry of a store that will not open.
	openBase time.Duration
	openMax  time.Duration

	// listGate bounds how many workers may be in their cold LIST at once, shared by the
	// whole fleet: enabling a cache arms a hundred kinds, and they must not all list at
	// one API server together. Standing watches are cheap and stay unbounded.
	listGate chan struct{}

	// runCtx bounds every worker; stop cancels it. Created up front so Track needs
	// no ordering against Start.
	runCtx    context.Context
	runCancel context.CancelFunc

	// mu guards tracked and published together: what a commit signals is measured
	// against who is still tracked.
	mu      sync.Mutex
	tracked map[string]*subject
	// published is the Reason each id's last signal carried, compared against so a
	// commit that moved only counts or timestamps wakes nobody.
	published map[string]string
	// heldCaches and heldSubjects are what a clear is holding stopped: Track refuses
	// while a hold stands, so a pass racing the clear cannot arm a worker that would
	// resume a watch into the file being emptied. Counted, since two clears of the same
	// cache may overlap.
	heldCaches   map[int64]int
	heldSubjects map[string]int
	// clearing counts every clear under way per cache, a per-kind one included. Holding
	// reads it: what the health fold must not mistake for a cache that stopped syncing
	// is any clear, while what may not arm a worker is only the subjects that clear
	// covers.
	clearing map[int64]int

	wg sync.WaitGroup
}

// subject is one tracked id: its params, this service's own claim on the context's
// connection, the running worker, and the worker's standing answer.
type subject struct {
	params Params
	lease  kubeconn.Lease
	// cancel stops this worker alone; done closes when it has exited. A restart and
	// Forget both wait on done, so a restarted worker never overlaps its
	// predecessor. Both are written under mu, since a restart replaces them.
	cancel context.CancelFunc
	done   chan struct{}
	// gen counts the restarts, so of two racing restarts waiting on the same worker only
	// the one that still holds the generation it waited on respawns it.
	gen   uint64
	obs   Observation
	known bool
}

// New returns a Service syncing over conns' connections into stores' files.
func New(conns connService, stores storeManager) *Service {
	return newWithOptions(conns, stores)
}

// option is a test seam, reachable only from white-box tests.
type option func(*Service)

// withOpenRetry shrinks the ladder a worker retries a failed store open up.
func withOpenRetry(base, max time.Duration) option {
	return func(s *Service) { s.openBase, s.openMax = base, max }
}

// withSync substitutes every worker's run body.
func withSync(f syncFunc) option {
	return func(s *Service) { s.sync = f }
}

// newWithOptions is New plus the seams.
func newWithOptions(conns connService, stores storeManager, opts ...option) *Service {
	runCtx, runCancel := context.WithCancel(context.Background())
	s := &Service{
		conns:        conns,
		stores:       stores,
		listGate:     make(chan struct{}, defaultListBound),
		signalHub:    conflate.New[string, struct{}](),
		runCtx:       runCtx,
		runCancel:    runCancel,
		tracked:      map[string]*subject{},
		published:    map[string]string{},
		heldCaches:   map[int64]int{},
		heldSubjects: map[string]int{},
		clearing:     map[int64]int{},
		openBase:     defaultBackoffBase,
		openMax:      defaultBackoffMax,
	}
	// The production body reads the fleet's own gate, so it is built after the struct
	// and before the options a test may replace it with.
	s.sync = func(ctx context.Context, p Params, lease kubeconn.Lease, store *kubestore.Store, commit func(Observation)) {
		newSyncer(p, lease, store, commit, withListGate(s.listGate)).run(ctx)
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Track starts a worker syncing id with p. A tracked id with the same params is left
// running, so a caller relaying spec churn restarts nothing; RestartAll is the restart
// that keeps them. Params that moved are a different sync — the kind's shape, the
// context, or the identity it must answer as — and a worker fixes all three at start,
// so the subject is replaced, its lease and its standing observation with it.
func (s *Service) Track(id string, p Params) {
	s.mu.Lock()
	if s.isHeld(id, p.CacheID) {
		s.mu.Unlock()
		return
	}
	if sub, held := s.tracked[id]; held {
		if sub.params == p {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		s.Forget(id)

		s.mu.Lock()
		// Forget waits for the old worker outside the lock, so the world may have moved:
		// another caller may have tracked id since — leave what is there rather than
		// replacing it again — and a clear may have taken the hold, which spawning now
		// would walk straight through.
		_, held := s.tracked[id]
		if held || s.isHeld(id, p.CacheID) {
			s.mu.Unlock()
			return
		}
	}
	// Acquire never fails and never waits, so holding mu across it costs nothing.
	sub := &subject{params: p, lease: s.conns.Acquire(p.ContextName), done: make(chan struct{})}
	s.tracked[id] = sub
	s.spawn(id, sub)
	s.mu.Unlock()
}

// isHeld reports whether a clear is holding this subject or its cache stopped. The
// caller that took the hold requeues the records it covers, and their passes arm the
// workers again once it is released. Called with mu held.
func (s *Service) isHeld(id string, cacheID int64) bool {
	return s.heldCaches[cacheID] > 0 || s.heldSubjects[id] > 0
}

// spawn starts sub's worker on its own child of s.runCtx, closing the done channel
// it was given as the goroutine exits. Called with mu held and sub already stored in
// s.tracked.
func (s *Service) spawn(id string, sub *subject) {
	ctx, cancel := context.WithCancel(s.runCtx)
	sub.cancel = cancel
	done := sub.done
	commit := s.commitFor(id, sub)
	lease, params := sub.lease, sub.params
	s.wg.Go(func() {
		defer close(done)
		s.runWorker(ctx, params, lease, commit)
	})
}

// runWorker holds this run's claim on the cache's store around the sync body: taken as
// the run starts, given back as it ends, so nothing keeps a file open for a subject
// that is not running — which is what lets a cache's teardown, having stopped the
// workers, delete it.
func (s *Service) runWorker(ctx context.Context, p Params, lease kubeconn.Lease, commit func(Observation)) {
	store := s.openStore(ctx, p, commit)
	if store == nil {
		return
	}
	defer store.Release()

	s.sync(ctx, p, lease, store, commit)
}

// openStore claims the cache's store, retrying up its own ladder until it opens or ctx
// ends. Nil means the worker has nothing to sync into and should stop.
//
// The retry is the worker's own because nothing else would try again: a descriptor limit
// or a full disk is transient, and the record cannot re-arm a subject whose params have
// not moved — Track leaves it exactly as it is.
func (s *Service) openStore(ctx context.Context, p Params, commit func(Observation)) *kubestore.Store {
	for delay := s.openBase; ; delay = min(delay*2, s.openMax) {
		store, err := s.stores.OpenOrCreate(p.CacheID)
		switch {
		case err == nil:
			return store
		case errors.Is(err, kubestore.ErrRemoved):
			// The cache is being torn down and this subject's Forget is on its way. There
			// is nothing to sync into and nothing worth reporting about a record that is
			// going.
			<-ctx.Done()
			return nil
		}
		commit(Observation{Reason: ReasonSyncFailed, Message: err.Error()})
		if sleep(ctx, delay) != nil {
			return nil
		}
	}
}

// commitFor binds a worker's commit closure to sub — checked, under mu, against
// what is currently tracked, so a commit racing a Forget or a restart cannot write
// or signal for a subject this service has already moved past.
func (s *Service) commitFor(id string, sub *subject) func(Observation) {
	return func(obs Observation) {
		s.mu.Lock()
		if s.tracked[id] != sub {
			s.mu.Unlock()
			return
		}
		sub.obs = obs
		sub.known = true
		changed := s.published[id] != obs.Reason
		if changed {
			s.published[id] = obs.Reason
		}
		s.mu.Unlock()

		if changed {
			s.signalHub.Sender().Send(id, struct{}{})
		}
	}
}

// Forget stops id's worker, waits for it to exit, releases its claim, and drops its
// observation. Unknown ids are a no-op.
func (s *Service) Forget(id string) {
	s.mu.Lock()
	sub := s.tracked[id]
	if sub == nil {
		s.mu.Unlock()
		return
	}
	delete(s.tracked, id)
	delete(s.published, id)
	cancel, done := sub.cancel, sub.done
	s.mu.Unlock()

	// Outside the lock: the worker's own commit takes mu, so waiting on done while
	// holding it would deadlock.
	cancel()
	<-done
	sub.lease.Release()
}

// WhileCacheStopped runs fn with cacheID's workers stopped and no new one able to start:
// a Track for one of them is refused until it returns. That is what a cache-wide clear
// needs, since stopping and emptying the file are two steps and a pass landing between
// them would leave a worker resuming a watch into the file the clear is about to swap —
// deltas into an empty database, with no cold list to fill it.
//
// The hold is released whatever fn returns: a cache nothing may arm is worse than one
// whose rows are still there. Callers requeue the records afterwards, since the passes
// refused meanwhile are what arm the workers again.
func (s *Service) WhileCacheStopped(cacheID int64, fn func() error) error {
	s.holdCache(cacheID, 1)
	defer s.holdCache(cacheID, -1)

	s.ForgetCache(cacheID)
	return fn()
}

// WhileStopped is WhileCacheStopped for one subject — the per-kind clear, whose rows and
// cookie go together. It takes the cache the subject syncs into as well, since the
// subject is forgotten by the time anything could look it up.
func (s *Service) WhileStopped(id string, cacheID int64, fn func() error) error {
	s.holdSubject(id, cacheID, 1)
	defer s.holdSubject(id, cacheID, -1)

	s.Forget(id)
	return fn()
}

// Holding reports whether a clear is holding cacheID's workers stopped. The health fold
// reads it so a clear — which stops every subject on its way through — is not mistaken
// for a cache that stopped syncing.
func (s *Service) Holding(cacheID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clearing[cacheID] > 0
}

// holdCache and holdSubject move a hold's count, dropping the entry at zero so the maps
// stay the live holds alone.
func (s *Service) holdCache(cacheID int64, delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heldCaches[cacheID] += delta; s.heldCaches[cacheID] <= 0 {
		delete(s.heldCaches, cacheID)
	}
	s.markClearing(cacheID, delta)
}

func (s *Service) holdSubject(id string, cacheID int64, delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heldSubjects[id] += delta; s.heldSubjects[id] <= 0 {
		delete(s.heldSubjects, id)
	}
	s.markClearing(cacheID, delta)
}

// markClearing moves the per-cache clear count both holds keep. Called with mu held.
func (s *Service) markClearing(cacheID int64, delta int) {
	if s.clearing[cacheID] += delta; s.clearing[cacheID] <= 0 {
		delete(s.clearing, cacheID)
	}
}

// Read returns id's standing observation, and whether its worker has committed one.
func (s *Service) Read(id string) (Observation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, tracked := s.tracked[id]
	if !tracked {
		return Observation{}, false
	}
	return sub.obs, sub.known
}

// Subscribe returns the change feed: the ids whose news moved.
func (s *Service) Subscribe() Subscription { return s.signalHub.Receiver() }

// Observations is the whole tracked fleet's standing answers, read in one critical
// section so a fold sees one consistent moment rather than a fleet mid-change.
func (s *Service) Observations() []SubjectObservation {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]SubjectObservation, 0, len(s.tracked))
	for id, sub := range s.tracked {
		out = append(out, SubjectObservation{ID: id, Params: sub.params, Observation: sub.obs, Known: sub.known})
	}
	return out
}

// restart replaces id's worker in place from its held params, keeping the last
// observation — a resume, not a teardown. Unknown ids are a no-op.
func (s *Service) restart(id string) {
	s.mu.Lock()
	sub := s.tracked[id]
	if sub == nil {
		s.mu.Unlock()
		return
	}
	gen, cancel, done := sub.gen, sub.cancel, sub.done
	s.mu.Unlock()

	// Outside the lock: the worker's own commit takes mu, so waiting on done while
	// holding it would deadlock.
	cancel()
	<-done

	s.mu.Lock()
	defer s.mu.Unlock()
	// The subject may have been Forgotten or replaced, or a racing restart that
	// waited on the same worker may have respawned it; respawn only what is still
	// tracked, and only from the generation this call ended, or two sync loops run
	// for one subject and only the later one is reachable to be stopped.
	if s.tracked[id] != sub || sub.gen != gen {
		return
	}
	// And never into a clear. A clear's own Forget removes the subject before it waits,
	// so the check above already covers the orderings it can produce — this states the
	// rule where it has to hold rather than leaving it to that coincidence, since a
	// worker respawned here would write into the file being closed and swapped.
	if s.isHeld(id, sub.params.CacheID) {
		return
	}
	sub.gen++
	sub.done = make(chan struct{})
	s.spawn(id, sub)
}

// ForgetCache is Forget for every subject syncing into cacheID, and like Forget it
// returns once those workers have exited — which is what a cache being torn down
// needs before its store is deleted: only a stopped worker cannot write through it.
func (s *Service) ForgetCache(cacheID int64) {
	for _, id := range s.idsForCache(cacheID) {
		s.Forget(id)
	}
}

// RestartAll restarts every tracked subject's worker — what a resume poke needs: each
// worker restarts in place off its cookie, so nothing re-lists a cluster that was
// only asleep. Sequential, and each waits for its worker, which is the point.
func (s *Service) RestartAll() {
	for _, id := range s.trackedIDs() {
		s.restart(id)
	}
}

// trackedIDs is every tracked subject, read in one critical section so the caller
// iterates a snapshot rather than the live map.
func (s *Service) trackedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(s.tracked))
	for id := range s.tracked {
		ids = append(ids, id)
	}
	return ids
}

// idsForCache is the subjects syncing into cacheID, read in one critical section so
// the caller iterates a snapshot rather than the live map.
func (s *Service) idsForCache(cacheID int64) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ids []string
	for id, sub := range s.tracked {
		if sub.params.CacheID == cacheID {
			ids = append(ids, id)
		}
	}
	return ids
}

// Start is the lifecycle shape. The workers run on the service's own context, so
// Start launches nothing; the stop func cancels them all and waits.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	return func(ctx context.Context) error {
		s.runCancel()
		return drain.WithContext(ctx, s.wg.Wait)
	}, nil
}

// Close releases what stop left: the tracked subjects' claims and the signal hub.
func (s *Service) Close() error {
	s.runCancel()

	s.mu.Lock()
	leases := make([]kubeconn.Lease, 0, len(s.tracked))
	for _, sub := range s.tracked {
		leases = append(leases, sub.lease)
	}
	clear(s.tracked)
	clear(s.published)
	s.mu.Unlock()

	for _, lease := range leases {
		lease.Release()
	}
	s.signalHub.Close()
	return nil
}
