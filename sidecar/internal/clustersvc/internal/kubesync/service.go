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

// Package kubesync fills the caches: it discovers what a cluster serves, mirrors every
// served kind into that cluster's kubestore file, and stands behind an answer about each
// one.
//
// It knows nothing about records. It speaks cache ids, kube-contexts, server UIDs and
// GVRs, and clustersvc translates — the layering rule every leaf under it follows. A
// record type reaching this package is an import cycle, which is the enforcement.
//
// Two levels of arming, and they AND rather than nest. TrackDiscovery/ForgetDiscovery say
// whether a cache syncs at all — they also supply the cache, since the connection claim
// and the store claim are taken there — and TrackKind/ForgetKind say which kinds. A
// registration outlives its cache being forgotten, which is what makes a pause one call
// and a resume one call, with no record written and none requeued.
//
// This file is the seam: arming, the gate between its two levels, the reads, the news
// feeds, and the lifecycle.
package kubesync

import (
	"context"
	"log/slog"
	"sync"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// Params is what one cache syncs: over which context, and as which server. A context is
// not an identity — it can be re-pointed at another cluster — so ServerUID is what makes
// a discovered kind and a mirrored object belong to this cache.
type Params struct {
	ContextName string
	ServerUID   string
}

// connService and storeManager are the two leaves this one stands on, named for the
// types they stand for and narrow enough that a test hands over a fake cluster.
type (
	connService interface {
		Acquire(contextName string) kubeconn.Lease
	}
	storeManager interface {
		OpenOrCreate(cacheID int64) (*kubestore.Store, error)
	}
)

// KindKey names one kind in one cache — the whole key of a kind's news, and everything a
// caller needs to address the record standing for it. It embeds the same value the
// methods take, so a caller composes one from what it already holds (note key.Kind.Kind
// is the singular; key.Kind is the embedded value).
//
// Carrying the singular is safe HERE, where a lookup would not tolerate it: a rename
// splits one key into two, and both translate to the same record name, so the cost is a
// duplicate wake and never a missed one.
type KindKey struct {
	CacheID int64
	kubestore.Kind
}

// DiscoveryNews is keyed by cache id, the whole address of the record it wakes.
type DiscoveryNews = *conflate.Receiver[int64, struct{}]

// KindNews is keyed by KindKey, which is the whole address of one kind's record.
type KindNews = *conflate.Receiver[KindKey, struct{}]

// kindID is what a kind is looked up by: the pair the server guarantees unique per
// group-version. The singular is data, not identity — a Kind renamed under an unchanged
// plural is the same collection, and keying on it too would read one worker as two.
type kindID struct{ apiVersion, resource string }

func idOf(k kubestore.Kind) kindID { return kindID{k.APIVersion, k.Resource} }

// kindRun is what one kind's sync is handed. It carries a Connection rather than a lease:
// the session has already waited for one vouching for its ServerUID, which is the gate
// nothing syncs past.
type kindRun struct {
	Kind   kubestore.Kind
	Conn   *kubeconn.Connection
	Store  *kubestore.Store
	Commit func(KindState)
}

// syncKindFn holds one kind's collection in step with the cluster: cold-list it into the
// store, then watch on from there, committing a KindState as it goes. It runs until its
// context ends and owns its own retry pacing — the loop above it re-enters it only after a
// restart, so returning promptly is asking to be run again.
type syncKindFn func(ctx context.Context, r kindRun)

// option is the test seam: the exported constructor takes production knobs only, and the
// sync is substituted from white-box tests. The sweep has none — it runs on the probe
// engine over whatever the connection reaches, so a test hands it an api server.
type option func(*Service)

func withSyncKindFn(f syncKindFn) option { return func(s *Service) { s.syncKindFn = f } }

// Service is the seam. One session per tracked cache holds that cache's claims and the
// workers under it; everything a caller reads is answered out of what those workers
// committed.
type Service struct {
	connSvc  connService
	storeMgr storeManager

	// discoveryEngine runs the three probes of discovery.go, one subject per armed cache.
	// The sweep alone rides it — a kind syncs on its own goroutine under a session.
	discoveryEngine *probe.Engine
	syncKindFn      syncKindFn

	// One news feed per worker, because their consumers are two beehive triggers and a
	// trigger wakes a record for every value its feed carries — one feed carrying both
	// would wake a cache for each of its hundreds of kinds. Each coalesces per key, so a
	// fleet syncing at once neither loses a cache behind a busier one nor overflows a
	// buffer. News is not a status: the key is the whole message, and the reader answers
	// it by re-reading.
	discoveryHub *conflate.Hub[int64, struct{}]
	kindHub      *conflate.Hub[KindKey, struct{}]

	// ctx bounds every worker in the process; the stop func cancels it. Built here rather
	// than in Start so a pass that arms a cache before the lifecycle has run is armed
	// rather than dropped.
	ctx    context.Context
	cancel context.CancelFunc

	// Two locks, one rule each. armMu serializes arming and forgetting, which wait for
	// workers to stop. mu guards everything a worker and a reader share — the maps below
	// plus each session's workers and committed states — and, because a worker's body
	// commits through it, is NEVER held while waiting for one.
	armMu sync.Mutex

	mu sync.Mutex
	// sessions holds the armed caches; tracked holds the registered kinds, which outlive
	// their cache being forgotten.
	sessions map[int64]*session
	tracked  map[int64]map[kindID]kubestore.Kind
	stopped  bool
}

// New returns a Service over the connection pool and the cache directory. Nothing is
// armed until a caller tracks it.
func New(connSvc connService, storeMgr storeManager, opts ...option) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		connSvc:         connSvc,
		storeMgr:        storeMgr,
		discoveryEngine: probe.New(),
		discoveryHub:    conflate.New[int64, struct{}](),
		kindHub:         conflate.New[KindKey, struct{}](),
		ctx:             ctx,
		cancel:          cancel,
		sessions:        map[int64]*session{},
		tracked:         map[int64]map[kindID]kubestore.Kind{},
	}
	for _, opt := range opts {
		opt(s)
	}
	registerProbes(s.discoveryEngine, s)
	s.discoveryEngine.OnPass(s.publishDiscovery)
	return s
}

// publishDiscovery projects what a pass left and records it, which is what wakes the cache
// when its verdict moved. Every pass, because the engine cannot tell a new answer from the
// same one re-confirmed — commitDiscovery is where that is decided.
func (s *Service) publishDiscovery(subject string, snap probe.Snapshot) {
	cacheID, ok := cacheIDOf(subject)
	if !ok {
		return
	}
	sess := s.sessionOf(cacheID)
	if sess == nil {
		return
	}
	partial, message := sess.lastSweep()
	s.commitDiscovery(sess, discoveryStateOf(snap, partial, message))
}

// TrackDiscovery arms cacheID's sweep, or updates it in place when the params move.
//
// **It is also what supplies the cache.** Params is what every worker dials over and the
// lease and store claim are taken here, so a kind tracked against a cache with no
// discovery has nothing to run on: this pair says whether a cache syncs at all, and
// TrackKind says which kinds.
//
// Arming is the caller's policy, never interest: nothing a reader does re-arms a cache
// the user paused.
func (s *Service) TrackDiscovery(cacheID int64, p Params) {
	s.armMu.Lock()
	defer s.armMu.Unlock()

	if sess := s.sessionOf(cacheID); sess != nil {
		if sess.params == p {
			return
		}
		// The identity or the credentials moved, so nothing the standing session holds is
		// the right claim any more.
		s.tearDown(cacheID)
	}
	s.arm(cacheID, p)
}

// ForgetDiscovery stops cacheID's workers and returns only once nothing can still write
// through that cache's store, which is what a teardown needs. The kinds stay registered:
// re-arming starts every one of them again, with no record written and none requeued.
func (s *Service) ForgetDiscovery(cacheID int64) {
	s.armMu.Lock()
	defer s.armMu.Unlock()
	s.tearDown(cacheID)
}

// TrackKind registers that one kind should be mirrored into cacheID, or updates its shape
// in place. A kind registered against a cache that is not armed is held rather than
// refused, so the record's pass and the cache's may land in either order.
func (s *Service) TrackKind(cacheID int64, k kubestore.Kind) {
	s.armMu.Lock()
	defer s.armMu.Unlock()

	id := idOf(k)
	s.mu.Lock()
	kinds, ok := s.tracked[cacheID]
	if !ok {
		kinds = map[kindID]kubestore.Kind{}
		s.tracked[cacheID] = kinds
	}
	prev, held := kinds[id]
	kinds[id] = k
	sess := s.sessions[cacheID]
	s.mu.Unlock()

	if sess == nil || (held && prev == k) {
		return
	}
	// A rename under an unchanged plural is the same collection, but a worker is armed
	// with the whole value — the rows are keyed by the singular — so it runs again.
	sess.stopKind(id)
	sess.dropKindState(id)
	sess.startKind(k)
}

// ForgetKind withdraws a kind's registration and waits for its worker.
func (s *Service) ForgetKind(cacheID int64, k kubestore.Kind) {
	s.armMu.Lock()
	defer s.armMu.Unlock()

	id := idOf(k)
	s.mu.Lock()
	if kinds, ok := s.tracked[cacheID]; ok {
		delete(kinds, id)
		if len(kinds) == 0 {
			delete(s.tracked, cacheID)
		}
	}
	sess := s.sessions[cacheID]
	s.mu.Unlock()

	if sess != nil {
		sess.stopKind(id)
		sess.dropKindState(id)
	}
}

// RestartAll restarts every armed worker in place, off its cookie — what a resume poke
// needs, since a watch that died under a sleeping machine reports nothing.
func (s *Service) RestartAll() {
	for _, sess := range s.sessionsSnapshot() {
		sess.restart()
	}
}

// GetDiscoveryState is cacheID's standing sweep answer. It reports false for what has not
// answered yet — a cache nobody has armed, or one whose sweep has committed nothing — and
// a caller that folds that into "serves no kinds" deletes a record set that was only
// waiting.
func (s *Service) GetDiscoveryState(cacheID int64) (DiscoveryState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[cacheID]
	if !ok || !sess.hasDiscoveryState {
		return DiscoveryState{}, false
	}
	return sess.discoveryState, true
}

// GetKindState is one mirror's standing answer, false until its worker has committed one.
func (s *Service) GetKindState(cacheID int64, k kubestore.Kind) (KindState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[cacheID]
	if !ok {
		return KindState{}, false
	}
	state, ok := sess.kindStates[idOf(k)]
	return state, ok
}

// WatchDiscoveryNews carries the caches whose sweep has something new to say. Close it
// when done.
func (s *Service) WatchDiscoveryNews() DiscoveryNews { return s.discoveryHub.Receiver() }

// WatchKindNews carries the kinds whose mirror has something new to say. Close it when
// done.
func (s *Service) WatchKindNews() KindNews { return s.kindHub.Receiver() }

// Start runs the probe engine the sweeps ride, and returns the func that cancels the
// workers and drains everything. The mirror workers are not launched here: one starts when
// a pass arms it, which may be before or after this.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	stopEngine := s.discoveryEngine.Start(ctx)

	return func(ctx context.Context) error {
		// armMu for the whole shutdown, like every other path that waits on workers: it is
		// what makes closing the door and draining one step. An arm in flight has published
		// its session but not yet launched its goroutines, so a drain that overlapped it
		// would join a session that does not hold its workers yet — and release claims it
		// has not taken.
		s.armMu.Lock()
		defer s.armMu.Unlock()

		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()

		s.cancel()
		// The engine first, and not through s.cancel: a run in flight holds the store claim
		// Close is about to give back, and the engine's loops are bounded by its own context.
		if err := stopEngine(ctx); err != nil {
			return err
		}
		return drain.WithContext(ctx, s.wait)
	}, nil
}

// wait joins every worker still running. Each session's own wait is what makes it safe to
// release the claims below.
func (s *Service) wait() {
	for _, sess := range s.sessionsSnapshot() {
		sess.wait()
	}
}

// sessionsSnapshot copies the armed sessions out from under mu, for a caller about to
// wait on their workers.
func (s *Service) sessionsSnapshot() []*session {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessions := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	return sessions
}

// Close releases the claims and the news hubs. Call after the stop func returns: a claim
// given back while a worker could still write through it is a write into a file nothing
// holds open.
func (s *Service) Close() error {
	s.armMu.Lock()
	defer s.armMu.Unlock()

	s.mu.Lock()
	sessions := s.sessions
	s.sessions = map[int64]*session{}
	s.stopped = true
	s.mu.Unlock()

	s.cancel()
	for _, sess := range sessions {
		sess.close()
	}
	s.discoveryHub.Close()
	s.kindHub.Close()
	return s.discoveryEngine.Close()
}

// arm builds the session for one cache and starts its sweep plus a worker per registered
// kind. Called under armMu.
//
// A session that will not start arms nothing and is retried by the next pass, which calls
// this again: no session means no answer, and "no answer" is what a caller must not fold
// into "serves no kinds" anyway. It is logged because nothing else would report it.
func (s *Service) arm(cacheID int64, p Params) {
	sess := newSession(s, cacheID, p)

	// The kinds and the session under one hold, so the replay below covers exactly the
	// registrations this session did not see arrive.
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	kinds := make([]kubestore.Kind, 0, len(s.tracked[cacheID]))
	for _, k := range s.tracked[cacheID] {
		kinds = append(kinds, k)
	}
	s.sessions[cacheID] = sess
	s.mu.Unlock()

	// Published before it holds anything, so a start that fails takes the entry back out —
	// a session in the map is what a run the engine schedules during start resolves through.
	// The context is the one thing a failed start leaves behind: nothing else was claimed,
	// but the child stays on s.ctx until it is cancelled, and this cache is armed again on
	// every pass.
	if err := sess.start(); err != nil {
		slog.Error("kubesync: arm cache", "cacheID", cacheID, "err", err)
		sess.cancel()
		s.mu.Lock()
		delete(s.sessions, cacheID)
		s.mu.Unlock()
		return
	}

	for _, k := range kinds {
		sess.startKind(k)
	}
}

// tearDown stops cacheID's session and gives its claims back. Called under armMu, and
// never under mu: stopping waits for bodies that commit through it.
func (s *Service) tearDown(cacheID int64) {
	s.mu.Lock()
	sess, ok := s.sessions[cacheID]
	delete(s.sessions, cacheID)
	s.mu.Unlock()
	if !ok {
		return
	}
	sess.close()
}

// sessionOf is the armed session for a cache, or nil.
func (s *Service) sessionOf(cacheID int64) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[cacheID]
}

// commitDiscovery records a sweep's answer and wakes the cache when its reason moved. A
// run that outlived its session commits nothing: the claims it wrote through are gone.
func (s *Service) commitDiscovery(sess *session, state DiscoveryState) {
	s.mu.Lock()
	if s.sessions[sess.cacheID] != sess {
		s.mu.Unlock()
		return
	}
	moved := !sess.hasDiscoveryState || sess.discoveryState.Reason != state.Reason
	sess.discoveryState, sess.hasDiscoveryState = state, true
	s.mu.Unlock()

	if moved {
		_ = s.discoveryHub.Sender().Send(sess.cacheID, struct{}{})
	}
}

// commitKind records one mirror's answer and wakes its record when the reason moved. A
// resume is not news: a watch re-established off its cookie changed nothing a reader can
// act on.
func (s *Service) commitKind(sess *session, k kubestore.Kind, state KindState) {
	id := idOf(k)
	s.mu.Lock()
	if s.sessions[sess.cacheID] != sess {
		s.mu.Unlock()
		return
	}
	prev, had := sess.kindStates[id]
	sess.kindStates[id] = state
	s.mu.Unlock()

	if !had || prev.Reason != state.Reason {
		_ = s.kindHub.Sender().Send(KindKey{CacheID: sess.cacheID, Kind: k}, struct{}{})
	}
}
