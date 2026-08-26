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
// when they move; Bounce is the restart — in place, from the held params, keeping the
// last observation — which is what the Clears and the poke resync both need: a worker
// whose cookie just died has nothing else to restart it.
// → docs/specs/cached-resource-sync.md.
package kubesync

import (
	"context"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
)

// connService is the pool the workers borrow connections from: a lease per tracked
// subject — refcounted in the pool beside every other holder's, so releasing one
// never stops the cluster being probed.
type connService interface {
	Acquire(contextName string) kubeconn.Lease
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
	// CacheID names the store the worker writes, and is the key BounceCache restarts
	// by. The subject id cannot carry it: the record's name embeds the catalog's
	// object id, not the cache's.
	CacheID     int64
	ContextName string
	ServerUID   string
	APIVersion  string
	Resource    string
	Namespaced  bool
}

// Observation is one worker's standing answer: the verdict and the freshness facts
// the health fold reads. Reason is the news — the signal fires only when it moves.
type Observation struct {
	Reason  string
	Message string
	// ObjectCount is the kind's cached rows as of the last write.
	ObjectCount int
	// LastUpdateAt is when data last arrived; LastLiveAt the last proof the watch is
	// live (a delta or a bookmark). Zero until the worker has one.
	LastUpdateAt time.Time
	LastLiveAt   time.Time
}

// syncFunc is one worker's whole run: sync p until ctx ends, publishing through
// commit (last call wins). The seam the tests substitute.
type syncFunc func(ctx context.Context, p Params, commit func(Observation))

// syncPlaceholder is the production body until the sync loop lands: it holds the
// subject open and publishes nothing, so a record folding it reads as still
// connecting.
func syncPlaceholder(ctx context.Context, _ Params, _ func(Observation)) {
	<-ctx.Done()
}

// Service runs the workers over the tracked ids.
type Service struct {
	conns connService
	sync  syncFunc
	// signalHub names the ids whose news changed, fed by each worker's commit.
	signalHub *conflate.Hub[string, struct{}]

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

	wg sync.WaitGroup
}

// subject is one tracked id: its params, this service's own claim on the context's
// connection, the running worker, and the worker's standing answer.
type subject struct {
	params Params
	lease  kubeconn.Lease
	// cancel stops this worker alone; done closes when it has exited. Bounce and
	// Forget both wait on done, so a restarted worker never overlaps its
	// predecessor. Both are written under mu, since a Bounce replaces them.
	cancel context.CancelFunc
	done   chan struct{}
	// gen counts the restarts, so of two Bounces waiting on the same worker only
	// the one that still holds the generation it waited on respawns it.
	gen   uint64
	obs   Observation
	known bool
}

// New returns a Service syncing over conns' connections.
func New(conns connService) *Service {
	return newWithOptions(conns)
}

// option is a test seam, reachable only from white-box tests.
type option func(*Service)

// withSync substitutes every worker's run body.
func withSync(f syncFunc) option {
	return func(s *Service) { s.sync = f }
}

// newWithOptions is New plus the seams.
func newWithOptions(conns connService, opts ...option) *Service {
	runCtx, runCancel := context.WithCancel(context.Background())
	s := &Service{
		conns:     conns,
		sync:      syncPlaceholder,
		signalHub: conflate.New[string, struct{}](),
		runCtx:    runCtx,
		runCancel: runCancel,
		tracked:   map[string]*subject{},
		published: map[string]string{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Track starts a worker syncing id with p. A tracked id with the same params is left
// running, so a caller relaying spec churn restarts nothing; Bounce is the restart
// that keeps them. Params that moved are a different sync — the kind's shape, the
// context, or the identity it must answer as — and a worker fixes all three at start,
// so the subject is replaced, its lease and its standing observation with it.
func (s *Service) Track(id string, p Params) {
	s.mu.Lock()
	if sub, held := s.tracked[id]; held {
		if sub.params == p {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		s.Forget(id)

		s.mu.Lock()
		// Forget waits for the old worker outside the lock, so another caller may have
		// tracked id since; leave what is there rather than replacing it again.
		if _, held := s.tracked[id]; held {
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

// spawn starts sub's worker on its own child of s.runCtx, closing the done channel
// it was given as the goroutine exits. Called with mu held and sub already stored in
// s.tracked.
func (s *Service) spawn(id string, sub *subject) {
	ctx, cancel := context.WithCancel(s.runCtx)
	sub.cancel = cancel
	done := sub.done
	s.wg.Go(func() {
		defer close(done)
		s.sync(ctx, sub.params, s.commitFor(id, sub))
	})
}

// commitFor binds a worker's commit closure to sub — checked, under mu, against
// what is currently tracked, so a commit racing a Forget or a Bounce cannot write
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

// Bounce restarts id's worker in place from its held params, keeping the last
// observation — a resume, not a teardown. Unknown ids are a no-op.
func (s *Service) Bounce(id string) {
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
	// The subject may have been Forgotten, replaced, or already restarted by a
	// Bounce that waited on the same worker; restart only what is still tracked, and
	// only from the generation this call ended, or two sync loops run for one
	// subject and only the later one is reachable to be stopped.
	if s.tracked[id] != sub || sub.gen != gen {
		return
	}
	sub.gen++
	sub.done = make(chan struct{})
	s.spawn(id, sub)
}

// BounceCache is Bounce for every subject syncing into cacheID — what a cleared
// cache needs, its workers' cookies having died with the file.
func (s *Service) BounceCache(cacheID int64) {
	for _, id := range s.idsForCache(cacheID) {
		s.Bounce(id)
	}
}

// ForgetCache is Forget for every subject syncing into cacheID, and like Forget it
// returns once those workers have exited — which is what a cache being torn down
// needs before its store is deleted: only a stopped worker cannot write through it.
func (s *Service) ForgetCache(cacheID int64) {
	for _, id := range s.idsForCache(cacheID) {
		s.Forget(id)
	}
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
