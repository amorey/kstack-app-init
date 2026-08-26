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

package kubesync

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
)

// One subject's sync: connect, build the kind if there is nothing to resume from, then
// watch — a standing push stream rather than a periodic pass, which is why the fleet
// runs its own goroutines instead of riding the probe engine.
//
// The cookie is what makes a restart cheap, and every use of the connection goes
// through ConnFor: a position is one cluster's etcd revision, so resuming against a
// context that was re-pointed would replay another cluster's history into this cache.

// Production cadences. Each is a parameter below whose production value is this
// constant, so a test picks its own timescale without encoding these.
const (
	// defaultPageSize bounds one LIST page — the memory a cold sync needs at once.
	defaultPageSize = 500
	// defaultStaleAfter is how long a caught-up watch may go without proving itself
	// alive before the verdict says the cache may be behind. Comfortably past the API
	// server's own bookmark cadence (~1m), so a healthy quiet cluster never flaps.
	defaultStaleAfter = 5 * time.Minute
	// defaultBackoffBase/Max pace the retry ladder after a failed attempt.
	defaultBackoffBase = time.Second
	defaultBackoffMax  = 30 * time.Second
	// defaultEventsWindow is how many events the cache keeps — the newest N, which
	// doubles as the events watch's diff window.
	defaultEventsWindow = 500
	// defaultEventsPruneTick ages events out of a cluster too quiet to prune on write.
	defaultEventsPruneTick = time.Minute
	// defaultListBound caps concurrent cold LISTs across a fleet, so enabling a cache
	// does not fire a hundred full lists at one API server. Standing watches are cheap
	// and unbounded.
	defaultListBound = 8
)

// collection is the one collection's REST surface a worker uses.
// dynamic.ResourceInterface satisfies it; a test substitutes its own.
type collection interface {
	List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

// collectionFunc resolves the collection one worker syncs over a connection. The seam
// the loop's tests substitute, since a live *kubeconn.Connection cannot be faked.
type collectionFunc func(conn *kubeconn.Connection, p Params) (collection, error)

// dynamicCollection is the production resolver: the kind's cluster-wide collection off
// the connection's dynamic client. Cluster-wide for a namespaced kind too — a cache
// mirrors the whole cluster.
func dynamicCollection(conn *kubeconn.Connection, p Params) (collection, error) {
	gv, err := schema.ParseGroupVersion(p.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q: %w", p.APIVersion, err)
	}
	return conn.Dynamic.Resource(gv.WithResource(p.Resource)), nil
}

// syncer is one subject's loop: what it syncs, where it writes, and how it reports.
type syncer struct {
	params Params
	lease  kubeconn.Lease
	store  *kubestore.Store
	commit func(Observation)

	collectionFor collectionFunc
	pageSize      int64
	staleAfter    time.Duration
	backoffBase   time.Duration
	backoffMax    time.Duration
	eventsWindow  int
	pruneTick     time.Duration
	// listGate bounds concurrent cold lists across the fleet; nil means unbounded.
	listGate chan struct{}

	// obs is the standing answer, republished whenever any of it moves. Held rather
	// than rebuilt so a Stale verdict keeps the freshness stamps that earned it.
	obs Observation
	// freshAt is what staleness is measured from: the last moment this kind's contents
	// were known good — a completed list, or traffic proving the stream live. It is not
	// LastLiveAt, which is a published claim about the STREAM and must not move on a
	// connect; this one starts running the moment a watch opens, so a stream that never
	// carries anything still goes stale.
	freshAt time.Time
	// carried counts what the WATCH has delivered — a delta or a bookmark. run compares
	// it across an attempt to tell a stream that worked and ended from one that opened
	// and gave nothing: both are a clean end, and they want different pacing. A listed
	// page does not count, since a loop that re-lists every time is the worse one.
	carried uint64
}

type syncOption func(*syncer)

func withCollection(fn collectionFunc) syncOption {
	return func(s *syncer) { s.collectionFor = fn }
}

func withStaleAfter(d time.Duration) syncOption {
	return func(s *syncer) { s.staleAfter = d }
}

func withBackoff(base, max time.Duration) syncOption {
	return func(s *syncer) { s.backoffBase, s.backoffMax = base, max }
}

func withEventsWindow(n int) syncOption {
	return func(s *syncer) { s.eventsWindow = n }
}

func withListGate(gate chan struct{}) syncOption {
	return func(s *syncer) { s.listGate = gate }
}

// newSyncer builds one subject's loop with the production cadences, then the options.
func newSyncer(p Params, lease kubeconn.Lease, store *kubestore.Store, commit func(Observation), opts ...syncOption) *syncer {
	s := &syncer{
		params:        p,
		lease:         lease,
		store:         store,
		commit:        commit,
		collectionFor: dynamicCollection,
		pageSize:      defaultPageSize,
		staleAfter:    defaultStaleAfter,
		backoffBase:   defaultBackoffBase,
		backoffMax:    defaultBackoffMax,
		eventsWindow:  defaultEventsWindow,
		pruneTick:     defaultEventsPruneTick,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// storeKind is the collection this subject mirrors, as the store names it. **The
// record's Kind, never the body's**: the rows a worker writes and the rows a teardown
// clears have to be keyed alike, and the teardown has only the record — a body claiming
// another Kind would leave rows under a name nothing will ever look up.
func (s *syncer) storeKind() kubestore.Kind {
	return kubestore.Kind{
		APIVersion: s.params.APIVersion,
		Kind:       s.params.Kind,
		Resource:   s.params.Resource,
	}
}

// run is the worker's whole life: attempts, paced by the backoff ladder, until ctx ends.
func (s *syncer) run(ctx context.Context) {
	backoff := s.backoffBase
	for ctx.Err() == nil {
		carried := s.carried
		err := s.attempt(ctx)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil && s.carried != carried:
			// The stream carried something, so the end was the server hanging up on a
			// working watch: the cookie is intact and the reopen is cheap. Still floored,
			// or a server closing right after each bookmark would spin every kind in the
			// cache against it.
			backoff = s.backoffBase
			if sleep(ctx, backoff) != nil {
				return
			}
		case err == nil:
			// It opened and gave us nothing. That is one reopen's cost per attempt, which
			// is nothing — and a hot loop per second against a proxy that accepts a watch
			// and hangs up, for every kind in the cache. Paced like a failure, without
			// being reported as one: nothing about the cache's contents changed.
			if sleep(ctx, backoff) != nil {
				return
			}
			backoff = min(backoff*2, s.backoffMax)
		default:
			s.publish(Observation{Reason: ReasonSyncFailed, Message: err.Error()})
			if sleep(ctx, backoff) != nil {
				return
			}
			backoff = min(backoff*2, s.backoffMax)
		}
	}
}

// attempt is one pass over one connection: build the kind if there is no position to
// resume from, then watch until the stream ends. A position the server refuses is
// answered here, by re-listing and watching on — the connection is still good, and a
// gap of unknown size has no other answer. A nil error is an end worth retrying at
// once; anything else goes up the ladder.
func (s *syncer) attempt(ctx context.Context) error {
	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	coll, err := s.collectionFor(conn, s.params)
	if err != nil {
		return err
	}
	rv, warm, err := s.store.Cookie(ctx, s.params.APIVersion, s.params.Resource)
	if err != nil {
		return err
	}
	s.obs.Resumed = warm
	if warm {
		// What the cache already holds. Nothing else reads it on this path — a resume
		// writes no page — and the fold's resync event promises the warm size.
		if err := s.refreshCount(ctx); err != nil {
			return err
		}
	}

	for ctx.Err() == nil {
		if !warm {
			// Only a build reports Syncing. A warm reopen — the server closing a watch on
			// its own timeout — publishes nothing here, or a healthy cache would walk
			// Watching → Syncing → Watching per reopen, waking its record twice and
			// writing a resync pair each time.
			s.publish(Observation{Reason: ReasonSyncing})
			if rv, err = s.coldSync(ctx, coll); err != nil {
				return err
			}
		}
		// listed: this attempt put the whole collection on disk a moment ago.
		err := s.watch(ctx, coll, rv, !warm)
		if !errors.Is(err, errExpired) {
			return err
		}
		// The position aged out. A re-list is the only way to close a gap of unknown
		// size, and a cache that already holds rows is resuming rather than building —
		// which is what the fold turns into a resync rather than a first sync.
		warm = false
		s.obs.Resumed = true
	}
	return ctx.Err()
}

// connect resolves the connection this cache's identity vouches for, reporting the
// refusal by its own reason and then waiting for one. Blocking here is legal — this is
// the worker's own goroutine, holding nothing shared; the prohibition is on probe runs.
func (s *syncer) connect(ctx context.Context) (*kubeconn.Connection, error) {
	conn, err := s.lease.ConnFor(ctx, s.params.ServerUID)
	if err == nil {
		return conn, nil
	}
	reason := ReasonNoConnection
	if errors.Is(err, kubeconn.ErrIdentityMismatch) {
		reason = ReasonIdentityMismatch
	}
	s.publish(Observation{Reason: reason, Message: err.Error()})

	return kubeconn.AwaitConnFor(ctx, s.lease, s.params.ServerUID)
}

// coldSync builds the kind from a paged LIST, returning the position to watch from.
// Pages land one transaction at a time, and the prune at the end is what removes rows
// that went while nothing was watching.
func (s *syncer) coldSync(ctx context.Context, coll collection) (string, error) {
	release, err := s.enterListGate(ctx)
	if err != nil {
		return "", err
	}
	defer release()

	var (
		session  *kubestore.ReplaceSession
		rv, next string
	)
	s.obs.ObjectCount = 0
	for {
		page, err := coll.List(ctx, metav1.ListOptions{
			Limit:    s.pageSize,
			Continue: next,
		})
		if err != nil {
			return "", fmt.Errorf("list %s: %w", s.params.Resource, err)
		}
		if session == nil {
			if session, err = s.store.BeginReplace(s.storeKind()); err != nil {
				return "", err
			}
		}
		items := make([]*unstructured.Unstructured, 0, len(page.Items))
		for i := range page.Items {
			items = append(items, &page.Items[i])
		}
		if err := session.WritePage(ctx, items); err != nil {
			return "", err
		}
		s.obs.ObjectCount += len(items)
		s.obs.LastUpdateAt = time.Now()
		s.publish(Observation{Reason: ReasonSyncing})

		rv = page.GetResourceVersion()
		if next = page.GetContinue(); next == "" {
			break
		}
	}

	if _, err := session.Commit(ctx, rv); err != nil {
		return "", err
	}
	// The collection is on disk as of this list, so staleness restarts from here.
	s.freshAt = time.Now()
	// A relist can carry more events than the cache keeps, so the window is applied
	// here too rather than waiting for the next event to arrive.
	if s.syncsEvents() {
		if err := s.pruneEvents(ctx); err != nil {
			return "", err
		}
	}
	if err := s.refreshCount(ctx); err != nil {
		return "", err
	}
	return rv, nil
}

// enterListGate holds the fleet's cold-list budget for the caller, returning the
// release. An unset gate is unbounded.
func (s *syncer) enterListGate(ctx context.Context) (func(), error) {
	if s.listGate == nil {
		return func() {}, nil
	}
	select {
	case s.listGate <- struct{}{}:
		return func() { <-s.listGate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// watch streams deltas from rv until the stream ends. A position the server refuses is
// the one end that must re-list: the gap is of unknown size, and only a full list can
// close it. Every other end returns for the ladder to pace.
func (s *syncer) watch(ctx context.Context, coll collection, rv string, listed bool) error {
	w, err := coll.Watch(ctx, metav1.ListOptions{
		ResourceVersion:     rv,
		AllowWatchBookmarks: true,
	})
	if err != nil {
		return fmt.Errorf("watch %s: %w", s.params.Resource, err)
	}
	defer w.Stop()

	// A connect proves nothing about the stream: a proxy can accept a watch and deliver
	// nothing, and reopening one inside the staleness threshold would keep a cache that
	// receives no updates looking healthy forever. So the verdict moves here only when
	// this attempt listed — the collection is on disk as of that list, which is what
	// "caught up" means — and LastLiveAt, which is a claim about the STREAM, moves only
	// on traffic.
	if listed {
		s.publish(Observation{Reason: ReasonWatching})
	}

	if s.freshAt.IsZero() {
		// Nothing has proven this kind good yet, so the clock starts here rather than at
		// the first proof — a stream that never carries anything has to go stale too.
		s.freshAt = time.Now()
	}
	// Armed for what is LEFT of the threshold, not a fresh one per connection: a proxy
	// accepting and closing an empty watch just inside it would otherwise restart the
	// clock on every reconnect, and a kind that last saw traffic hours ago would read
	// healthy forever.
	stale := time.NewTimer(s.untilStale())
	defer stale.Stop()
	// The age-out tick, for a cluster too quiet to prune on write. Only the events
	// kind ages out; a nil channel leaves the branch inert for every other worker.
	var pruneC <-chan time.Time
	if s.syncsEvents() {
		prune := time.NewTicker(s.pruneTick)
		defer prune.Stop()
		pruneC = prune.C
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-stale.C:
			// A verdict, not a teardown: the stream stands and the rows stand with it.
			// Only what the cache claims about its own freshness changes.
			s.publish(Observation{Reason: ReasonStale})
			stale.Reset(s.staleAfter)

		case <-pruneC:
			if err := s.pruneEvents(ctx); err != nil {
				return err
			}

		case ev, ok := <-w.ResultChan():
			if !ok {
				// The server hung up. The cookie stands, so the next attempt resumes.
				return nil
			}
			if err := s.apply(ctx, ev); err != nil {
				return err
			}
			stale.Reset(s.untilStale())
			s.publish(Observation{Reason: ReasonWatching})
		}
	}
}

// untilStale is how long the last proof of life has left before the watch is stale, and
// zero once it is past — so a reconnect inherits the deadline rather than resetting it. A
// worker that has never had a proof starts the threshold from now: there is nothing to
// date staleness from yet.
func (s *syncer) untilStale() time.Duration {
	return max(s.staleAfter-time.Since(s.freshAt), 0)
}

// apply lands one watch event. A Bookmark carries a position and nothing else — it is
// the proof of life a quiet collection has, and writing it as an object would invent a
// row.
func (s *syncer) apply(ctx context.Context, ev watch.Event) error {
	now := time.Now()

	switch ev.Type {
	case watch.Bookmark:
		// A bookmark is the proof a quiet collection has; an error frame below is the
		// stream reporting a failure, and stamping for it would let a proxy that answers
		// every watch with one keep the cache reading fresh forever.
		s.live(now)
		if u, ok := ev.Object.(*unstructured.Unstructured); ok {
			if rv := u.GetResourceVersion(); rv != "" {
				return s.store.SetCookie(ctx, s.params.APIVersion, s.params.Resource, rv)
			}
		}
		return nil

	case watch.Error:
		return watchError(ev)

	case watch.Added, watch.Modified, watch.Deleted:
		s.live(now)
		u, ok := ev.Object.(*unstructured.Unstructured)
		if !ok {
			// A body the dynamic client could not shape is not something a retry fixes,
			// and dropping one object is better than stalling the collection.
			return nil
		}
		if err := s.store.ApplyChange(ctx, s.storeKind(), ev.Type, u); err != nil {
			return err
		}
		s.obs.LastUpdateAt = now
		if s.syncsEvents() {
			if err := s.pruneEvents(ctx); err != nil {
				return err
			}
		}
		return s.refreshCount(ctx)

	default:
		return nil
	}
}

// live records that the stream delivered something: the freshness the health gauge
// reads, and the count run compares across an attempt.
func (s *syncer) live(at time.Time) {
	s.obs.LastLiveAt = at
	s.freshAt = at
	s.carried++
}

// watchError turns the stream's error frame into this loop's own: an expired position is
// the one that must re-list, and it is reported as a clean end so the next attempt runs
// at once rather than up the ladder.
//
// Through apierrors.FromObject rather than a type assertion, because the shape depends on
// the decoder: one that knows metav1 hands back a Status, one that does not hands back
// the same document as an unstructured object. Reading only the typed one would leave a
// worker retrying a position the server refuses forever.
func watchError(ev watch.Event) error {
	err := apierrors.FromObject(ev.Object)
	if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
		return errExpired
	}
	return fmt.Errorf("watch failed: %w", err)
}

// errExpired is a position the server refused for being too old. Handled inside the
// attempt rather than by the ladder: the answer is a re-list, which is what the next
// attempt does once the cookie is gone.
var errExpired = errors.New("watch resourceVersion expired")

// syncsEvents reports the one kind whose rows age out.
func (s *syncer) syncsEvents() bool {
	return s.params.APIVersion == "v1" && s.params.Resource == "events"
}

// pruneEvents keeps the events window a window; only a delete pings the events bus, so
// this is what an events watch learns an age-out from.
func (s *syncer) pruneEvents(ctx context.Context) error {
	_, err := s.store.PruneEvents(ctx, s.eventsWindow)
	return err
}

// refreshCount re-reads the kind's tally, which is O(1) off the trigger-maintained
// counts.
func (s *syncer) refreshCount(ctx context.Context) error {
	n, err := s.store.CountKind(ctx, s.storeKind())
	if err != nil {
		return err
	}
	s.obs.ObjectCount = n
	return nil
}

// publish updates the standing answer's verdict and hands it to the fleet, which
// signals only when the reason moves — so a count tick or a timestamp costs a copy
// rather than a requeue.
func (s *syncer) publish(next Observation) {
	s.obs.Reason = next.Reason
	s.obs.Message = next.Message
	s.commit(s.obs)
}

// sleep waits d, or returns ctx's error if the worker is stopped first.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
