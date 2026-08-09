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

// Machinery for running kubesync workers on behalf of beehive objects: the registry, the
// drain-before-forget rule, the out-of-band report guard, and Status → condition/event
// translation. ClusterCacheGVRSyncController is its only caller; the shape generalises to
// any future worker-owning kind.

const (
	// Bounds one teardown so a wedged drain can't block a reconcile worker or shutdown.
	workerStopTimeout = 10 * time.Second

	// Steady-state re-reconcile cadence; detects a credential rotation.
	workerRecheckInterval = 30 * time.Second

	// Bounds concurrent per-kind sync reconciles. Sized for count, not latency: a cache has
	// one object per served kind, so a startup pass walks hundreds.
	gvrSyncConcurrency = 8
)

// workerHandle is a controller's view of one running sync worker (*kubesync.Worker in
// production; a seam so tests drive the lifecycle without the network).
type workerHandle interface {
	Start()
	Stop(ctx context.Context) error
}

// syncEntry is the runtime state for one running worker. Its pointer identity is the
// sink's guard: a report from a stopped or replaced worker is dropped by comparing it
// against the set.
type syncEntry struct {
	worker workerHandle
	// Connection-config fingerprint the worker started with; a change is a credential
	// rotation and restarts it.
	fingerprint string
	objID       beehive.ObjectID
	// Object generation at worker start, used as observedGeneration on condition writes.
	gen int64
	// Build inputs, kept so a resync poke can rebuild without a reconcile. Copied fields,
	// not a closure, which would pin everything else in scope for the worker's life.
	// cdb is the handle this worker HOLDS — a restart re-opens, so it is not what the next
	// worker will write through.
	cfg  *rest.Config
	cdb  *store.ClusterDB
	kind objectsync.Kind
	// Locates the cache written into, so a restart rebuilds against the same one and a
	// cache-scoped restart can tell which workers are its own.
	ref store.CacheRef

	// Set the moment a stop begins, so reports from a worker on its way out are dropped
	// while its entry is still registered (it stays until the drain succeeds — see stop).
	// Read from the sink's goroutine, hence atomic.
	draining atomic.Bool

	// Last reported (state, cause). The event log records only a change in it, so the ~30s
	// heartbeat refreshes stamps without appending "Watching ×27". Mutated under writeMu.
	lastState kubesync.State
	lastCause kubesync.Cause
}

// noun names what this object syncs ("events", "deployments") — the plural resource, so it
// can't fall out of step with the kind.
func (e *syncEntry) noun() string { return e.kind.Resource }

// workerSet is a controller's registry of running workers, keyed by object id.
type workerSet struct {
	// Guards workers/lifecycle. Held only around map access, never across a start/stop.
	mu      sync.Mutex
	workers map[beehive.ObjectID]*syncEntry
	// One lock per object, held across a whole stop-then-start SEQUENCE, not each half.
	// putIfAbsent alone is not enough: the gap between a stop and its start is a moment
	// when the object legitimately has no worker, and a pause or deletion landing there
	// reports itself done — clearing the drain finalizer, releasing the cache controller's
	// barrier — while the restart puts a worker back for an object that is paused or gone,
	// writing into a .db being deleted. See docs/adr/2026-08-09-beehive-control-plane.md.
	//
	// Per object, not shared: a startup pass walks hundreds independently. Reclaimed only
	// on the deletion path (forgetLifecycle) — ids are AUTOINCREMENT and never reused, so
	// every migration or CRD churn mints a fresh ~150.
	lifecycle map[beehive.ObjectID]*sync.Mutex
	// Counts in-flight RestartCacheWorkers sequences per CacheID. The lifecycle locks can't
	// cover this alone: they come from a SNAPSHOT, so a reconcile that opened the cache but
	// hasn't registered its worker holds a lock nobody knows to wait for. Registration
	// checks this under the same mutex the snapshot is taken under, which is what makes
	// "every worker of this cache is either drained or refused" true.
	//
	// A count, not a flag: begin/end pairs nest.
	restarting map[int64]int
	// Serializes whole restart sequences per cache — see cacheRestartGate.
	restartGates map[int64]*cacheRestartGate
}

// cacheRestartGate is a context-aware mutex for one cache's restart sequences, held from
// the snapshot through the restarts.
//
// Serializing them keeps the sequence's two halves consistent. Overlapping sequences take
// lifecycle locks from separate snapshots and hold them to the end, so they can deadlock
// permanently (sync.Mutex has no timeout); and even when they don't, the second's snapshot
// is stale — holds() rejects every entry, and it deletes the cache file while the first's
// fresh workers write to it.
//
// A channel rather than a sync.Mutex because the wait must honour the caller's context
// (ClearCache runs under a timeout).
type cacheRestartGate struct {
	// Buffered-1 semaphore; holding its token means holding the gate.
	ch chan struct{}
	// Holder plus everyone blocked on it, so the map entry drops only when unused.
	waiters int
}

// errCacheRestarting refuses a registration whose handle the in-flight restart is about to
// close; the caller drops the worker and lets the next reconcile rebuild it.
var errCacheRestarting = errors.New("cache is restarting its workers")

func newWorkerSet() *workerSet {
	return &workerSet{
		workers:      make(map[beehive.ObjectID]*syncEntry),
		lifecycle:    make(map[beehive.ObjectID]*sync.Mutex),
		restarting:   make(map[int64]int),
		restartGates: make(map[int64]*cacheRestartGate),
	}
}

// acquireCacheRestart takes cacheID's restart gate, blocking until the holding sequence
// finishes or ctx ends. The returned release is idempotent and must be called on every path.
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

// dropCacheRestartWaiter drops one reference, reclaiming the gate once unused — cache ids
// are never reused, so otherwise a long-lived process accumulates one gate per clear.
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

// beginCacheRestart marks cacheID restarting AND snapshots its workers in one critical
// section. The atomicity is the point: a worker registered before is in the snapshot (so the
// caller drains it), one registering after is refused — no third case. Pair with
// endCacheRestart. Ordered by object id so every sequence takes lifecycle locks alike.
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

// endCacheRestart ends ONE sequence's hold; call exactly once per beginCacheRestart.
func (s *workerSet) endCacheRestart(cacheID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.restarting[cacheID] - 1; n > 0 {
		s.restarting[cacheID] = n
	} else {
		delete(s.restarting, cacheID)
	}
}

// lifecycleLock returns objID's lock, creating it on first use. Hold it across every
// stop/start meant to be atomic — see the field comment.
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

// forgetLifecycle drops objID's lock. Callable ONLY from the finalize path, by that lock's
// holder, once the object is collected: a later caller would otherwise mint a fresh mutex
// and fail to exclude a goroutine still blocked on the old one — and after collection there
// is no later caller.
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

// putIfAbsent registers entry, reporting whether it did. The two refusals differ:
// (false, nil) means the object already has a worker — a raced restart drops the one it
// built rather than displacing a newer one, so nothing retries. errCacheRestarting means
// the worker was built on a handle about to close: don't start it; another reconcile must.
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

// isCurrent reports whether entry is still its object's live worker — neither replaced nor
// draining. Out-of-band restarts use it to skip work somebody else took over.
func (s *workerSet) isCurrent(entry *syncEntry) bool {
	if entry.draining.Load() {
		return false
	}
	return s.holds(entry)
}

// holds reports whether entry is still registered, WITHOUT isCurrent's draining test. They
// differ for one case: a worker whose stop timed out stays registered and draining. Asking
// "is this worker gone" needs this one — a draining entry may still be writing, so reading
// isCurrent's false as "somebody else handled it" skips a live writer.
func (s *workerSet) holds(entry *syncEntry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers[entry.objID] == entry
}

// stopBounded is stop under workerStopTimeout — the form every reconcile path wants.
func (s *workerSet) stopBounded(ctx context.Context, objID beehive.ObjectID) error {
	stopCtx, cancel := context.WithTimeout(ctx, workerStopTimeout)
	defer cancel()
	return s.stop(stopCtx, objID) //nolint:contextcheck // stopCtx is ctx plus a bound
}

// stop drains objID's worker and forgets it — but ONLY once the drain actually succeeded.
//
// Marking draining rather than deleting up front does two jobs: it drops in-flight reports
// immediately, and it keeps a FAILED drain visible, which is what makes the deletion
// barrier real. A timed-out drain that forgot its worker would let the next reconcile clear
// the drain finalizer while the wedged goroutine still writes to a .db about to be deleted.
// See docs/adr/2026-08-09-beehive-control-plane.md.
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

// stopAll stops every worker and waits. Called at shutdown AFTER beehive drains (so no
// reconcile starts another) and BEFORE the cache manager, since workers write into it.
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

// syncSink folds one worker's reports into its object, out of band from the reconcile path.
// The entry pointer drops reports from a stopped or replaced worker; writeMu serializes
// against the reconcile path's writes to the same object.
type syncSink struct {
	set     *workerSet
	entry   *syncEntry
	writeMu *sync.Mutex
	// Called under writeMu, only for the object's live worker.
	apply func(ctx context.Context, entry *syncEntry, st kubesync.Status) error
	// Labels a failed fold in the log.
	name string
}

func (s *syncSink) Report(st kubesync.Status) {
	if !s.set.isCurrent(s.entry) {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Re-check under writeMu: the worker may have been replaced since, and a stale report
	// must not clobber the new one's status.
	if !s.set.isCurrent(s.entry) {
		return
	}
	if err := s.apply(context.Background(), s.entry, st); err != nil {
		slog.Warn(s.name+": fold worker report", "object", s.entry.objID, "err", err)
	}
}

// writeSyncGate records a reconcile-level (not worker-level) Synced condition — paused, or
// waiting on a connection. Conditions only: freshness stamps aren't ours to touch when we
// aren't running. Takes writeMu to serialize against the worker sinks.
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

// syncedCondition maps one report onto the Synced condition; noun is needed only by Stale.
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

// recordSyncTransition appends one report to the object's beehive event log, ONLY on a
// change in the (state, cause) pair — the ~30s heartbeat would otherwise grow a steady run
// into "Watching ×27", and its only new information (the stamps) is served out of band.
// Best-effort: a failure doesn't advance the recorded state, so the next report retries.
// Must hold the caller's writeMu.
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

// syncEvent maps one report onto an event's (type, reason, message) — the transition
// vocabulary, distinct from syncedCondition's current-state naming. Each phase splits on
// ColdStart into a start/complete pair; prev is the state transitioned FROM, which
// distinguishes a first catch-up from a recovery out of Stale.
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

// resyncCompleteMessage describes a finished warm resume. Deliberately omits the cache
// total: a clean resume re-fetches nothing, so "N in 0s" would misread as instant work.
func resyncCompleteMessage(st kubesync.Status, noun string) string {
	if st.Resynced {
		return fmt.Sprintf("Re-sync complete — saved position expired, re-synced %d %s in %s",
			st.ResyncedItems, noun, roundSyncDuration(st.CaughtUpIn))
	}
	return fmt.Sprintf("Re-sync complete — resumed the watch in %s", roundSyncDuration(st.CaughtUpIn))
}

// staleMessage names the cause so the message is actionable (e.g. grant `watch`).
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

// roundSyncDuration rounds to a tenth of a second so messages read as "in 4.2s".
func roundSyncDuration(d time.Duration) time.Duration {
	return d.Round(100 * time.Millisecond)
}

// keepStamp carries a stamp forward when a report doesn't advance it: an opening report
// carries neither, and a quiet collection carries no LastUpdateAt for hours.
func keepStamp(next, prev *time.Time) *time.Time {
	if next != nil {
		return next
	}
	return prev
}
