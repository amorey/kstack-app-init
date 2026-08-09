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

package cluster

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/objectsync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// This file holds the machinery for running kubesync workers on behalf of beehive
// objects: the worker registry, the drain-before-forget rule, the out-of-band report
// guard, and the translation from a kubesync.Status into a condition and an event-log
// entry. ClusterCacheGVRSyncController is its only caller.
//
// It is a separate file because it is a separate concern from the reconcile — the
// controller decides WHETHER a kind's worker should run, this decides what running one
// entails — and because the shape generalises: a future worker-owning kind reuses it by
// supplying its own build func and its own noun.

const (
	// workerStopTimeout bounds one worker teardown, so a wedged drain can't block a
	// reconcile worker (or shutdown) indefinitely.
	workerStopTimeout = 10 * time.Second

	// workerRecheckInterval paces the steady-state re-reconcile of a running worker, which
	// is what detects a credential rotation (a changed connection fingerprint).
	workerRecheckInterval = 30 * time.Second

	// gvrSyncConcurrency bounds how many per-kind sync reconciles beehive runs at once.
	// Unlike clusterProbeConcurrency this isn't about one slow cluster blocking others —
	// it's about count: a cache has one ClusterCacheGVRSync per served kind, so a startup
	// pass has hundreds of objects to walk, each opening the cache and starting a worker.
	gvrSyncConcurrency = 8
)

// workerHandle is a controller's view of one running sync worker — satisfied by
// *kubesync.Worker. The seam lets tests drive the lifecycle and the report path without
// touching the network.
type workerHandle interface {
	Start()
	Stop(ctx context.Context) error
}

// syncEntry is a controller's runtime state for one running worker. The pointer is the
// sink's guard: a report from a stopped or replaced worker is dropped by comparing
// pointer identity against the set.
type syncEntry struct {
	worker workerHandle
	// fingerprint of the connection config the worker was started with; a change is a
	// credential rotation and restarts it.
	fingerprint string
	objID       beehive.ObjectID
	// gen is the object's generation when the worker started, used as the
	// observedGeneration on its condition writes.
	gen int64
	// The inputs this worker was built from, kept so a resync poke can rebuild it without
	// a reconcile. Copied fields rather than a closure: a closure would pin whatever else
	// happened to be in scope at ensureWorker for the worker's whole life.
	//
	// cdb is the handle the worker HOLDS, recorded rather than reused: a restart re-opens
	// (the handle may have been closed since — see startEntry), so this is what the worker
	// is writing through, not what the next one will.
	cfg  *rest.Config
	cdb  *store.ClusterDB
	kind objectsync.Kind
	// ref locates the cache this worker writes into. Kept so a restart can rebuild against
	// the same cache — and so a cache-scoped restart can tell which workers are its own.
	ref store.CacheRef

	// draining is set the moment a stop begins, so reports from a worker on its way out
	// are dropped even while its entry is still in the set (it stays until the drain
	// succeeds — see stop). Read from the sink's goroutine, hence atomic.
	draining atomic.Bool

	// lastState/lastCause are the last reported (state, cause) pair. The event log records
	// only a change in it, so the worker's 30s freshness heartbeat refreshes the stamps
	// without appending a redundant "Watching ×27" run. Mutated only under the owner's
	// writeMu.
	lastState kubesync.State
	lastCause kubesync.Cause
}

// noun is what this object's messages call what it syncs ("events", "deployments") — the
// plural resource, so it needs no separate field to fall out of step with the kind.
func (e *syncEntry) noun() string { return e.kind.Resource }

// workerSet is a controller's registry of running workers, keyed by object id.
type workerSet struct {
	// mu guards workers/lifecycle. Held only around map access, never across a worker
	// start/stop.
	mu      sync.Mutex
	workers map[beehive.ObjectID]*syncEntry
	// lifecycle holds one lock per object, taken across a whole stop-then-start SEQUENCE
	// rather than around each half.
	//
	// putIfAbsent alone is not enough. It stops a restart from displacing a newer worker,
	// but the window between a stop and the start that follows it is a moment when the
	// object legitimately has no worker — and a reconcile landing there reads that as "no
	// worker to stop" and proceeds: a pause reports Paused, and a deletion clears the drain
	// finalizer, which releases the cache controller's barrier and lets the .db file be
	// removed. The restart then puts a worker back for an object that is paused or gone,
	// writing into a file being deleted, with nothing left to reconcile it away.
	//
	// Per object, not one shared lock: distinct kinds' lifecycles are independent, and a
	// startup pass walks hundreds of them. Reclaimed on the deletion path only — see
	// forgetLifecycle. Unlike the core controller's per-cluster map (bounded by the
	// kube-contexts ever seen, so tens), this one is keyed by ClusterCacheGVRSync ids,
	// which are AUTOINCREMENT and never reused: every server-UID migration, cluster
	// delete/recreate or CRD churn mints a fresh set of ~150.
	lifecycle map[beehive.ObjectID]*sync.Mutex
	// restarting counts the RestartCacheWorkers sequences in flight per CacheID. The
	// per-object lifecycle locks cannot cover this on their own: they are taken from a
	// SNAPSHOT of the running workers, so a reconcile that has opened the cache but not yet
	// registered its worker owns a lock nobody knows to wait for. Registration checks this
	// under the same mutex the snapshot is taken under, which is what makes "every worker of
	// this cache is either drained or refused" true.
	//
	// A COUNT, not a flag: begin/end pairs nest, so an inner hold can't be dropped by an
	// outer one finishing first. Whole sequences are serialized per cache by restartGates,
	// so in practice this is 0 or 1.
	restarting map[int64]int
	// restartGates serializes whole RestartCacheWorkers sequences per cache — see
	// cacheRestartGate.
	restartGates map[int64]*cacheRestartGate
}

// cacheRestartGate is a context-aware mutex for one cache's restart sequences, held from
// the snapshot through the restarts.
//
// Serializing them is what keeps the sequence's two halves consistent. Two overlapping
// sequences take their per-object lifecycle locks from separate snapshots, in map-iteration
// order, and hold them all to the end — so they can deadlock outright (each waiting on a
// lock the other holds), and sync.Mutex has no timeout, so a deadlock is permanent. Even
// when the orders happen to agree, the second sequence's snapshot is stale by the time it
// runs: every entry in it was already drained and replaced, holds() rejects them all, and
// it deletes the cache file with the first sequence's fresh workers writing to it.
//
// A channel rather than a sync.Mutex because the wait must honour the caller's context:
// ClearCache runs under a timeout, and blocking past it is the failure this replaces.
type cacheRestartGate struct {
	// ch is a buffered-1 semaphore; holding its one token means holding the gate.
	ch chan struct{}
	// waiters counts the holder plus everyone blocked on it, so the map entry is dropped
	// only once nobody is using it.
	waiters int
}

// errCacheRestarting refuses a worker registration while its cache is mid-restart. The
// handle it was built on is the one about to be closed, so the caller must drop the worker
// and let the next reconcile rebuild it against whatever handle the restart leaves behind.
var errCacheRestarting = errors.New("cache is restarting its workers")

func newWorkerSet() *workerSet {
	return &workerSet{
		workers:      make(map[beehive.ObjectID]*syncEntry),
		lifecycle:    make(map[beehive.ObjectID]*sync.Mutex),
		restarting:   make(map[int64]int),
		restartGates: make(map[int64]*cacheRestartGate),
	}
}

// acquireCacheRestart takes cacheID's restart gate, blocking until the sequence holding it
// finishes or ctx ends. It returns the release, which is idempotent and must be called on
// every path out.
func (s *workerSet) acquireCacheRestart(ctx context.Context, cacheID int64) (func(), error) {
	s.mu.Lock()
	g, ok := s.restartGates[cacheID]
	if !ok {
		g = &cacheRestartGate{ch: make(chan struct{}, 1)}
		s.restartGates[cacheID] = g
	}
	g.waiters++
	s.mu.Unlock()

	select {
	case g.ch <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-g.ch
				s.dropCacheRestartWaiter(cacheID)
			})
		}, nil
	case <-ctx.Done():
		s.dropCacheRestartWaiter(cacheID)
		return nil, ctx.Err()
	}
}

// dropCacheRestartWaiter drops one reference to cacheID's gate, reclaiming it once the last
// holder or waiter is gone. Reclaiming matters for the same reason lifecycle does: cache ids
// are AUTOINCREMENT and never reused, so a long-lived process would otherwise accumulate one
// gate per cache it ever cleared.
func (s *workerSet) dropCacheRestartWaiter(cacheID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.restartGates[cacheID]
	if !ok {
		return
	}
	if g.waiters--; g.waiters <= 0 {
		delete(s.restartGates, cacheID)
	}
}

// beginCacheRestart marks cacheID as restarting AND snapshots that cache's running workers,
// in one critical section. The atomicity is the point: a worker registered before this call
// is in the returned snapshot (so the caller drains it), and one registering after is
// refused by putIfAbsent — with no third case in between. Pair with endCacheRestart.
//
// The snapshot is ordered by object id rather than left in map order, so every sequence
// takes the lifecycle locks in the same order.
func (s *workerSet) beginCacheRestart(cacheID int64) []*syncEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.restarting[cacheID]++
	var mine []*syncEntry
	for _, entry := range s.workers {
		if entry.ref.CacheID == cacheID {
			mine = append(mine, entry)
		}
	}
	slices.SortFunc(mine, func(a, b *syncEntry) int { return cmp.Compare(a.objID, b.objID) })
	return mine
}

// endCacheRestart ends ONE sequence's hold. Call it exactly once per beginCacheRestart —
// the caller's release func is what guarantees that, since the barrier is lifted before the
// restarts and again on the way out.
func (s *workerSet) endCacheRestart(cacheID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.restarting[cacheID] - 1; n > 0 {
		s.restarting[cacheID] = n
	} else {
		delete(s.restarting, cacheID)
	}
}

// lifecycleLock returns objID's lifecycle lock, creating it on first use. A caller holds it
// across every stop/start it means to be atomic — see the field comment.
func (s *workerSet) lifecycleLock(objID beehive.ObjectID) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	mu, ok := s.lifecycle[objID]
	if !ok {
		mu = &sync.Mutex{}
		s.lifecycle[objID] = mu
	}
	return mu
}

// forgetLifecycle drops objID's lifecycle lock. Callable ONLY from the finalize path, by
// the holder of that very lock, once the object has been collected — which is what makes
// it safe. A goroutine already blocked on the lock holds its own reference and still runs
// to completion after we release it; the danger is only that a LATER caller would mint a
// fresh mutex and so fail to exclude that one, and after collection there is no later
// caller (nothing enumerates a gone object, and the set holds no worker for it).
func (s *workerSet) forgetLifecycle(objID beehive.ObjectID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.lifecycle, objID)
}

func (s *workerSet) get(objID beehive.ObjectID) *syncEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers[objID]
}

// putIfAbsent registers entry, reporting whether it did. It refuses two ways, and the
// difference matters to the caller.
//
// A false with no error means the object already HAS a worker — how an out-of-band restart
// hands back to a reconcile that raced it: the poke drains the old worker, and if a
// reconcile started a replacement in the gap, the poke drops the one it built rather than
// displacing a newer one. Nothing is wrong, so nothing retries.
//
// errCacheRestarting means the cache is mid-restart and this worker was built on the handle
// that restart is about to close. That IS an error: the caller must not start the worker,
// and the object needs another reconcile to get one.
func (s *workerSet) putIfAbsent(entry *syncEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restarting[entry.ref.CacheID] > 0 {
		return false, errCacheRestarting
	}
	if _, exists := s.workers[entry.objID]; exists {
		return false, nil
	}
	s.workers[entry.objID] = entry
	return true, nil
}

// entries returns a snapshot of the running workers.
func (s *workerSet) entries() []*syncEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Collect(maps.Values(s.workers))
}

// isCurrent reports whether entry is still its object's live worker — neither replaced by
// a newer one nor already draining. The out-of-band restart (a resync poke) uses it to skip
// work somebody else has taken over.
func (s *workerSet) isCurrent(entry *syncEntry) bool {
	if entry.draining.Load() {
		return false
	}
	return s.holds(entry)
}

// holds reports whether entry is still the set's registered worker for its object, WITHOUT
// isCurrent's draining test. The two differ for exactly one entry: a worker whose stop
// timed out, which stays registered and flagged draining.
//
// A caller that must know "is this worker gone" needs this one, not isCurrent. A draining
// entry is not gone — its goroutine may still be writing — so reading isCurrent's false as
// "somebody else handled it" is how a live writer gets skipped. See RestartCacheWorkers.
func (s *workerSet) holds(entry *syncEntry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers[entry.objID] == entry
}

// stopBounded is stop under workerStopTimeout — the form every reconcile path wants, so a
// wedged drain can't hold a reconcile worker indefinitely.
func (s *workerSet) stopBounded(ctx context.Context, objID beehive.ObjectID) error {
	stopCtx, cancel := context.WithTimeout(ctx, workerStopTimeout)
	defer cancel()
	return s.stop(stopCtx, objID) //nolint:contextcheck // stopCtx is ctx plus a bound
}

// stop drains the worker for objID, if any, and forgets it — but ONLY once the drain has
// actually succeeded.
//
// Marking the entry draining (rather than deleting it up front) does two jobs. It drops
// in-flight reports immediately, so a draining worker can't write status behind us; and it
// keeps a FAILED drain visible, which is what makes the deletion barrier real. If a
// timed-out drain forgot its worker, the next deletion reconcile would find nothing to
// stop, report success, and clear the drain finalizer — releasing the cache controller's
// wait while the wedged goroutine still holds the ClusterDB handle and writes to a .db
// that is about to be deleted. Keeping the entry means the next attempt re-waits on the
// same worker instead.
func (s *workerSet) stop(ctx context.Context, objID beehive.ObjectID) error {
	entry := s.get(objID)
	if entry == nil {
		return nil
	}
	entry.draining.Store(true)
	if err := entry.worker.Stop(ctx); err != nil {
		return fmt.Errorf("stop sync worker: %w", err)
	}
	s.mu.Lock()
	// Only clear the entry we drained: a concurrent start may already have replaced it.
	if s.workers[objID] == entry {
		delete(s.workers, objID)
	}
	s.mu.Unlock()
	return nil
}

// stopAll stops every running worker and waits for them to unwind. The service calls it
// during shutdown AFTER beehive has drained (so no reconcile can start another) and
// BEFORE the cache manager shuts down, since a worker writes into the cache it holds.
func (s *workerSet) stopAll(ctx context.Context) error {
	s.mu.Lock()
	ids := make([]beehive.ObjectID, 0, len(s.workers))
	for id := range s.workers {
		ids = append(ids, id)
	}
	s.mu.Unlock()

	var errs []error
	for _, id := range ids {
		if err := s.stop(ctx, id); err != nil { //nolint:contextcheck // shutdown ctx bounds the whole drain
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// syncSink folds one worker's reports into its object, out of band from the reconcile
// path. It holds the entry pointer so reports from a stopped or replaced worker are
// dropped, and takes the owning controller's writeMu so a report serializes against the
// reconcile path's own writes to the same object.
type syncSink struct {
	set     *workerSet
	entry   *syncEntry
	writeMu *sync.Mutex
	// apply folds one report into the object. Called under writeMu, and only for a report
	// from the object's live worker.
	apply func(ctx context.Context, entry *syncEntry, st kubesync.Status) error
	// name labels a failed fold in the log (the controller this sink belongs to).
	name string
}

func (s *syncSink) Report(st kubesync.Status) {
	if !s.set.isCurrent(s.entry) {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Re-check under writeMu: the worker could have been replaced between the check above
	// and acquiring the lock, and a stale report must not clobber the new one's status.
	if !s.set.isCurrent(s.entry) {
		return
	}
	if err := s.apply(context.Background(), s.entry, st); err != nil {
		slog.Warn(s.name+": fold worker report", "object", s.entry.objID, "err", err)
	}
}

// writeSyncGate records a reconcile-level (not worker-level) Synced condition — paused, or
// waiting on a connection. A gate report is conditions and nothing else: the freshness
// stamps are not ours to touch when we aren't running. It takes the owning controller's
// writeMu because it serializes against the worker sinks, which write the same object out
// of band.
func writeSyncGate[Status any](
	ctx context.Context,
	client beehive.ControllerClient[Status],
	writeMu *sync.Mutex,
	objID beehive.ObjectID,
	generation int64,
	reason, message string,
) error {
	writeMu.Lock()
	defer writeMu.Unlock()
	return reportCondition(ctx, client, objID, generation,
		liveCondition(ConditionSynced, ConditionFalse, reason, message))
}

// syncedCondition maps one worker report onto the Synced condition. noun is what is being
// synced, which only a Stale report's message needs.
func syncedCondition(st kubesync.Status, noun string) Condition {
	switch st.State {
	case kubesync.StateWatching:
		return liveCondition(ConditionSynced, ConditionTrue, ReasonWatching, "")
	case kubesync.StateErrored:
		return liveCondition(ConditionSynced, ConditionFalse, ReasonSyncFailed, st.LastError)
	case kubesync.StateStale:
		return liveCondition(ConditionSynced, ConditionFalse, ReasonStale, staleMessage(st.Cause, noun))
	default:
		return liveCondition(ConditionSynced, ConditionFalse, ReasonSyncing, "")
	}
}

// recordSyncTransition appends one report to the object's beehive event log — the
// sync-side parallel of the connection controller's attempt log. It records only on a
// TRANSITION (a change in the reported (state, cause) pair): a worker re-reports its state
// on a ~30s freshness heartbeat, and recording every one would grow a steady Watching run
// into a meaningless "Watching ×27" while its only new information — the stamps — is
// served out of band anyway. Best-effort: a failure is logged and does not advance the
// recorded state, so the next report retries. Must hold the caller's writeMu.
func recordSyncTransition[Status any](
	ctx context.Context,
	client beehive.ControllerClient[Status],
	entry *syncEntry,
	st kubesync.Status,
) {
	if entry.lastState == st.State && entry.lastCause == st.Cause {
		return
	}
	typ, reason, message := syncEvent(st, entry.lastState, entry.noun())
	err := client.AddEvent(ctx, entry.objID, beehive.EventSpec{
		Category: SyncEventCategory,
		Type:     typ,
		Reason:   reason,
		Message:  truncateMessage(message),
	})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("cachesync: record sync event", "object", entry.objID, "reason", reason, "err", err)
		}
		return
	}
	entry.lastState, entry.lastCause = st.State, st.Cause
}

// syncEvent maps one report onto a beehive event's (type, reason, message) — the event
// log's transition vocabulary, distinct from syncedCondition (which names the current
// state). Each phase splits on ColdStart into a start/complete pair: a Syncing report is
// SyncStart (cold) or ResyncStart (warm), and catching up is SyncComplete (cold) or
// ResyncComplete (warm). prev is the state we're transitioning FROM, which is what
// distinguishes a first catch-up from a recovery out of Stale. noun is what is being
// synced ("events", "deployments") — the one thing these messages need to know.
func syncEvent(st kubesync.Status, prev kubesync.State, noun string) (beehive.EventType, string, string) {
	switch st.State {
	case kubesync.StateWatching:
		if prev == kubesync.StateStale || prev == kubesync.StateErrored {
			return beehive.EventNormal, ReasonResyncComplete,
				fmt.Sprintf("Watch recovered — streaming %s again", noun)
		}
		if st.ColdStart {
			return beehive.EventNormal, ReasonSyncComplete, fmt.Sprintf(
				"Initial sync complete — cached %d %s in %s",
				st.SyncedItems, noun, roundSyncDuration(st.CaughtUpIn))
		}
		return beehive.EventNormal, ReasonResyncComplete, resyncCompleteMessage(st, noun)
	case kubesync.StateErrored:
		return beehive.EventWarning, ReasonSyncDegraded, st.LastError
	case kubesync.StateStale:
		return beehive.EventWarning, ReasonSyncStale, staleMessage(st.Cause, noun)
	default: // StateSyncing
		if st.ColdStart {
			return beehive.EventNormal, ReasonSyncStart, fmt.Sprintf("Starting initial %s sync", noun)
		}
		return beehive.EventNormal, ReasonResyncStart, fmt.Sprintf(
			"Starting re-sync from warm cache — %d %s, resuming the watch from the saved position",
			st.CachedItems, noun)
	}
}

// resyncCompleteMessage describes a finished warm resume. It deliberately does NOT report
// the cache's total: a clean resume re-opens the watch and re-fetches nothing (items
// stream in as deltas), so an "N in 0s" line would misread as "processed N instantly". The
// honest fact is whether the resume was clean or had to re-list, and how long it took.
func resyncCompleteMessage(st kubesync.Status, noun string) string {
	if st.Resynced {
		return fmt.Sprintf("Re-sync complete — saved position expired, re-synced %d %s in %s",
			st.ResyncedItems, noun, roundSyncDuration(st.CaughtUpIn))
	}
	return fmt.Sprintf("Re-sync complete — resumed the watch in %s", roundSyncDuration(st.CaughtUpIn))
}

// staleMessage explains a stale report by naming its cause, so the message is actionable
// (e.g. grant `watch` on the resource) rather than a bare "not receiving updates".
func staleMessage(cause kubesync.Cause, noun string) string {
	switch cause {
	case kubesync.CauseListFailed:
		return fmt.Sprintf("Not receiving %s — the list is failing; the cache may be behind", noun)
	case kubesync.CauseWatchFailed:
		return fmt.Sprintf("Not receiving %s — the watch cannot be established; the cache may be behind", noun)
	case kubesync.CauseWatchStalled:
		return fmt.Sprintf("Not receiving %s — the watch has stalled; the cache may be behind", noun)
	case kubesync.CauseWatchNotStreaming:
		return fmt.Sprintf("Not receiving %s — the watch keeps reconnecting without streaming; the cache may be behind", noun)
	default:
		return fmt.Sprintf("Not receiving %s — the cache may be behind", noun)
	}
}

// roundSyncDuration rounds a catch-up duration to a tenth of a second so the event
// message stays stable and readable (e.g. "in 4.2s").
func roundSyncDuration(d time.Duration) time.Duration {
	return d.Round(100 * time.Millisecond)
}

// keepStamp returns next when the report advanced the stamp, else the value already held.
// A worker's opening report carries neither stamp, and a quiet collection's reports carry
// no LastUpdateAt for hours — neither should blank what a previous run earned.
func keepStamp(next, prev *time.Time) *time.Time {
	if next != nil {
		return next
	}
	return prev
}
