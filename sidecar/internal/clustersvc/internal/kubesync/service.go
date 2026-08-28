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
	"time"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/supervisor"
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
// plural is the same collection, and keying on it too would read one kind as two.
type kindID struct{ apiVersion, resource string }

func idOf(k kubestore.Kind) kindID { return kindID{k.APIVersion, k.Resource} }

// option is the test seam: the exported constructor takes production knobs only, and the
// kind reconciler is substituted from white-box tests. The sweep has none — both run on a
// supervisor over whatever the connection reaches, so a test hands it an api server.
type option func(*Service)

// withKindReconciler substitutes the reconciler every kind subject runs, for a test about
// arming rather than about what a kind sync reads. It is handed the Service because a
// substitute takes the same admission the real one does.
func withKindReconciler(build func(*Service) supervisor.Reconciler[*kindStream]) option {
	return func(s *Service) { s.buildKindReconciler = build }
}

// withPacing shrinks what a kind sync runs on, so no test outwaits a production number.
func withPacing(p pacing) option { return func(s *Service) { s.pacing = p } }

// Service is the seam. One session per tracked cache holds that cache's claims and the runs
// and streams under it; everything a caller reads is answered out of what those committed.
type Service struct {
	connSvc  connService
	storeMgr storeManager

	// Two supervisors, because their subjects are different things. discoverySupervisor runs
	// the three probes of discovery.go over one subject per armed cache; kindSupervisor runs
	// the kind reconciler over one subject per kind, where a run establishes the stream and
	// commits it as its value rather than being it.
	discoverySupervisor *supervisor.Supervisor
	kindSupervisor      *supervisor.Supervisor
	buildKindReconciler func(*Service) supervisor.Reconciler[*kindStream]
	pacing              pacing

	// One news feed per level, because their consumers are two beehive triggers and a
	// trigger wakes a record for every value its feed carries — one feed carrying both
	// would wake a cache for each of its hundreds of kinds. Each coalesces per key, so a
	// fleet syncing at once neither loses a cache behind a busier one nor overflows a
	// buffer. News is not a status: the key is the whole message, and the reader answers
	// it by re-reading.
	discoveryHub *conflate.Hub[int64, struct{}]
	kindHub      *conflate.Hub[KindKey, struct{}]

	// ctx bounds every run and stream in the process; the stop func cancels it. Built here
	// rather than in Start so a pass that arms a cache before the lifecycle has run is armed
	// rather than dropped.
	ctx    context.Context
	cancel context.CancelFunc

	// Two locks, one rule each. armMu serializes arming and forgetting, which wait for runs
	// and streams to stop. mu guards everything a run and a reader share — the maps below
	// plus each session's runs and committed states — and, because a run commits through it,
	// is NEVER held while waiting for one.
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
		connSvc:             connSvc,
		storeMgr:            storeMgr,
		discoverySupervisor: supervisor.New(),
		discoveryHub:        conflate.New[int64, struct{}](),
		kindHub:             conflate.New[KindKey, struct{}](),
		ctx:                 ctx,
		cancel:              cancel,
		sessions:            map[int64]*session{},
		tracked:             map[int64]map[kindID]kubestore.Kind{},
	}
	s.pacing = defaultPacing()
	for _, opt := range opts {
		opt(s)
	}
	s.kindSupervisor = supervisor.New(supervisor.WithWorkers(s.pacing.kindSyncWorkers))
	if s.buildKindReconciler == nil {
		s.buildKindReconciler = func(s *Service) supervisor.Reconciler[*kindStream] {
			return kindReconciler{s: s, pacing: s.pacing}
		}
	}
	registerProbes(s.discoverySupervisor, s)
	s.discoverySupervisor.OnPass(s.publishDiscovery)
	registerKindSync(s.kindSupervisor, s)
	s.kindSupervisor.OnPass(s.publishKind)
	return s
}

// registerKindSync wires the kind reconciler. WithTimeout(0) because a cold list of a large
// kind legitimately outlasts any bound the reads want, and the interval is a liveness re-check
// — the stream's own exit is the real re-entry.
func registerKindSync(e *supervisor.Supervisor, s *Service) {
	supervisor.Register(e, nameKindSync, s.buildKindReconciler(s),
		supervisor.WithInterval(kindSyncInterval),
		supervisor.WithBackoff(s.pacing.backoff.Base, s.pacing.backoff.Factor, s.pacing.backoff.Cap),
		supervisor.WithTimeout(0))
}

// publishDiscovery projects what a pass left and records it, which is what wakes the cache
// when its verdict moved. Every pass, because the supervisor cannot tell a new answer from the
// same one re-confirmed — commitDiscovery is where that is decided.
func (s *Service) publishDiscovery(subject string, snap supervisor.Snapshot) {
	cacheID, ok := parseDiscoverySubject(subject)
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
// **It is also what supplies the cache.** Params is what every run dials over and the lease
// and store claim are taken here, so a kind tracked against a cache with no discovery has
// nothing to run on: this pair says whether a cache syncs at all, and TrackKind says which
// kinds.
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

// ForgetDiscovery stops everything under cacheID and returns only once nothing can still
// write through that cache's store, which is what a teardown needs. The kinds stay registered:
// re-arming starts every one of them again, with no record written and none requeued.
func (s *Service) ForgetDiscovery(cacheID int64) {
	s.armMu.Lock()
	defer s.armMu.Unlock()
	s.tearDown(cacheID)
}

// TrackKind registers that one kind should be synced into cacheID, or updates its shape in
// place. A kind registered against a cache that is not armed is held rather than
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
	// A rename under an unchanged plural is the same collection, but the stream and the run
	// in flight were both started with the whole value — the rows are keyed by the singular —
	// so this kind comes down and goes up again. Stopped before it is armed, so the generation
	// that keys rows by the old singular is gone before the one that keys them by the new.
	s.stopKind(sess, id)
	s.kindSupervisor.Add(kindSubject(cacheID, k))
}

// ForgetKind withdraws a kind's registration and returns only once nothing can still write
// through it: the stream is joined and the run in flight is cancelled and joined.
func (s *Service) ForgetKind(cacheID int64, k kubestore.Kind) {
	// Held across the join, like every other arming path. Releasing it first would let a
	// TrackKind for this kind land while the withdrawn run was still unwinding: the kind would
	// read as tracked again, so commitKind would stop refusing that run's late writes, and its
	// replacement would be dispatched alongside it.
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

	s.stopKind(sess, id)
}

// stopKind takes one kind down and waits for it: its subject is removed — which hands its stream
// back, cancel and join — the run in flight is cancelled and joined, and its verdict is dropped
// last. Past here nothing can write through this kind, which is what makes forgetting synchronous
// and a rename safe to re-arm behind.
//
// The cancel is what bounds the join: what remains of a run is a page request unwinding, not
// the cold list it was in the middle of.
func (s *Service) stopKind(sess *session, id kindID) {
	if sess == nil {
		return
	}
	s.kindSupervisor.Remove(kindSubject(sess.cacheID, kubestore.Kind{
		APIVersion: id.apiVersion, Resource: id.resource,
	}))
	sess.cancelKindRun(id).join()
	// **After the join, and that is what makes it final.** A rename leaves the kind tracked
	// under its new singular, so commitKind refuses nothing the old generation reports on its
	// way down — dropped any earlier, that report would re-create the entry and serve a
	// withdrawn generation's verdict for the one that replaced it, before it has listed a row.
	sess.dropKindState(id)
}

// publishKind overlays what the supervisor knows onto what the kind's syncer committed. The
// ladder, the count and the countdown are the supervisor's; the reason is the syncer's except
// where an attempt outranks it.
func (s *Service) publishKind(subject string, snap supervisor.Snapshot) {
	cacheID, id, ok := parseKindSubject(subject)
	if !ok {
		return
	}
	sess := s.sessionOf(cacheID)
	if sess == nil {
		return
	}
	a := snap.Attempts(nameKindSync)
	// Which attempts outrank the syncer: a suspension is why nothing is syncing, and a failure
	// is what went wrong. A rotation outranks nothing — the rows stay current across it.
	outranks := a.LastAttempt.Verdict == supervisor.VerdictSuspended ||
		(a.LastAttempt.Verdict == supervisor.VerdictFailed && a.LastAttempt.Reason != reasonWatchRotated)

	if _, had := sess.kindState(id); !had && !outranks {
		// The syncer has said nothing and the attempt has nothing to add: a kind armed but not
		// yet answered stands behind no answer, and the seam promises the getter says so.
		// Publishing the overlay alone would invent one and wake its record for nothing.
		return
	}

	s.commitKindState(sess, id, func(state KindState, _ bool) KindState {
		// Restarts is the streak, which only a frame ends — the establishing run is
		// Provisional, so it climbs across every death until the stream proves itself.
		state.Restarts = a.Failures
		// A healthy stream has a liveness re-check scheduled, and the seam promises this is
		// zero while one is up: only a run that is down is retrying.
		state.NextRetryAt = time.Time{}
		if a.LastAttempt.Verdict == supervisor.VerdictFailed {
			state.NextRetryAt = a.NextAttempt.ScheduledAt
		}
		if outranks {
			reason := a.LastAttempt.Reason
			if a.LastAttempt.Verdict == supervisor.VerdictFailed {
				reason = ReasonSyncFailed
			}
			state.setReason(reason, a.LastAttempt.Message)
		}
		// A succeeded attempt and a rotation both overlay nothing: the syncer's answer stands,
		// which is what keeps a resume — and a reopen — holding Watching.
		return state
	})
}

// RestartAll restarts every armed kind in place, off its cookie — what a resume poke needs,
// since a watch that died under a sleeping machine reports nothing.
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

// GetKindState is one kind's standing answer, false until it has committed one.
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

// WatchKindNews carries the kinds with something new to say. Close it when done.
func (s *Service) WatchKindNews() KindNews { return s.kindHub.Receiver() }

// Start runs the two supervisors, and returns the func that cancels every run and drains
// everything. No cache is armed here: one is armed when a pass tracks it, which may be before
// or after this.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	stopDiscovery := s.discoverySupervisor.Start(ctx)
	stopKinds := s.kindSupervisor.Start(ctx)

	return func(ctx context.Context) error {
		// armMu for the whole shutdown, like every other path that waits on runs: it is what
		// makes closing the door and draining one step. An arm in flight has published its
		// session but not yet launched its goroutines, so a drain that overlapped it would
		// join a session that does not hold them yet — and release claims it has not taken.
		s.armMu.Lock()
		defer s.armMu.Unlock()

		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()

		s.cancel()
		// The supervisors first, and not through s.cancel: a run in flight holds the store claim
		// Close is about to give back, and their loops are bounded by their own contexts.
		if err := stopKinds(ctx); err != nil {
			return err
		}
		if err := stopDiscovery(ctx); err != nil {
			return err
		}
		return drain.WithContext(ctx, s.wait)
	}, nil
}

// wait joins every run and stream still up. Each session's own wait is what makes it safe to
// release the claims below.
func (s *Service) wait() {
	for _, sess := range s.sessionsSnapshot() {
		sess.wait()
	}
}

// sessionsSnapshot copies the armed sessions out from under mu, for a caller about to act on
// them outside it.
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
// given back while a run could still write through it is a write into a file nothing holds
// open.
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
	if err := s.kindSupervisor.Close(); err != nil {
		return err
	}
	return s.discoverySupervisor.Close()
}

// arm builds the session for one cache and starts its sweep plus a subject per registered
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
	// a session in the map is what a run the supervisor schedules during start resolves through.
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
		s.kindSupervisor.Add(kindSubject(cacheID, k))
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

// commitKind records one kind's answer and wakes its record when the reason moved — a resume
// is not news, since a watch re-established off its cookie changed nothing a reader can act
// on. A run that outlived its session or its registration writes nothing: the claims it wrote
// through are gone, and a kind nobody tracks has no reader to tell — which is what lets
// ForgetKind return without ordering anything after the join.
func (s *Service) commitKind(sess *session, id kindID, state KindState) {
	s.commitKindState(sess, id, func(stored KindState, had bool) KindState {
		// **The syncer owns the reason and the stamps, never the countdown.** Restarts and
		// NextRetryAt are the supervisor's, projected by publishKind, and this copy was taken
		// when the run began — writing it back would restore the countdown of the failure it
		// recovered from, and leave a healthy stream reading as one that is retrying.
		if had {
			state.Restarts, state.NextRetryAt = stored.Restarts, stored.NextRetryAt
		}
		return state
	})
}

// commitKindState is the one writer of what a reader sees for a kind. Its two callers own
// different fields of the same answer, so each hands in a merge rather than a whole state:
// applied under the lock, so neither can lose the other's write.
func (s *Service) commitKindState(sess *session, id kindID, merge func(stored KindState, had bool) KindState) {
	s.mu.Lock()
	k, tracked := s.tracked[sess.cacheID][id]
	if s.sessions[sess.cacheID] != sess || !tracked {
		s.mu.Unlock()
		return
	}
	prev, had := sess.kindStates[id]
	state := merge(prev, had)
	sess.kindStates[id] = state
	s.mu.Unlock()

	if !had || prev.Reason != state.Reason {
		_ = s.kindHub.Sender().Send(KindKey{CacheID: sess.cacheID, Kind: k}, struct{}{})
	}
}
