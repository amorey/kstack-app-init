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

// One kind's mirror: a cold LIST or a resume from the cookie, then a WATCH applying deltas
// until something ends it. A standing stream rather than a pass, which is why this does not
// run on the probe engine.
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
	"k8s.io/client-go/dynamic"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// errWatchClosed is the apiserver ending a watch on its own timeout: the ordinary end of a
// stream rather than a failure of one.
var errWatchClosed = errors.New("watch closed")

// pacing is every duration and bound a mirror runs on. A test shrinks them; production
// passes defaultPacing, so no test outwaits a production number.
type pacing struct {
	// staleAfter is how long a watch may go without proving itself alive before its verdict
	// reads Stale. A resume that outlasts it says Resuming for the same reason: both are
	// "this stream has stopped being current".
	staleAfter time.Duration
	// backoff is the ladder a failing run climbs — the engine's, so a kind's retry countdown
	// reads the same as a sweep's.
	backoff probe.Backoff
	// pageSize bounds one relist page, so a large collection never lands in memory whole.
	pageSize int64
	// eventsWindow is how many event rows the cache keeps, and eventsEvery how many arrive
	// between prunes: the statement scans the table, so paying it per delta would make a
	// busy cluster's event stream quadratic.
	eventsWindow int
	eventsEvery  int
	// coldLists bounds relists in flight across the process: arming a cache arms hundreds
	// of kinds at once, and each holds a connection and the store's single writer.
	coldLists chan struct{}
}

func defaultPacing() pacing {
	return pacing{
		staleAfter:   5 * time.Minute,
		backoff:      probe.Backoff{Base: time.Second, Factor: 2, Cap: time.Minute},
		pageSize:     500,
		eventsWindow: 5000,
		eventsEvery:  100,
		coldLists:    make(chan struct{}, 8),
	}
}

// syncKindWith builds the mirror every kind worker runs. The gate is built once, with the
// pacing, so every worker in the process queues on the same one.
func syncKindWith(p pacing) syncKindFn {
	return func(ctx context.Context, r kindRun) {
		(&kindSync{run: r, pacing: p}).loop(ctx)
	}
}

// kindSync is one entry into the mirror. What must outlive it — the verdict, the stamps, the
// restart count — is read back through run.Prev, since a restart re-enters the body with a
// fresh one of these.
type kindSync struct {
	run    kindRun
	pacing pacing

	state KindState
	// sincePrune counts event deltas applied since the last prune.
	sincePrune int
	// relist forces the next attempt to cold-list, for the one failure a resume cannot retry
	// its way out of: the position the cookie names is the position the server dropped.
	relist bool
}

// loop runs until its context ends, pacing its own retries: the worker above re-enters this
// the moment it returns, so a run that gives up promptly would spin.
func (w *kindSync) loop(ctx context.Context) {
	w.state, _ = w.run.Prev()

	for ctx.Err() == nil {
		err := w.establish(ctx)
		if ctx.Err() != nil {
			return
		}
		if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
			w.relist = true
		}
		// The ladder climbs the STREAK, which the first frame of a working stream cleared: a
		// closure hours after the last one is the first failure of its own streak and waits the
		// base, where counting every failure this worker ever had would creep to the cap and stay.
		w.state.Restarts++
		delay := w.pacing.backoff.Delay(w.state.Restarts)
		w.state.NextRetryAt = time.Now().Add(delay)
		if errors.Is(err, errWatchClosed) {
			// A rotation is not news: the server closed the watch cleanly and the rows stay
			// current across the reopen, where reporting it would flicker every kind through
			// SyncFailed every few minutes. The verdict stands and the countdown is published
			// under it, so a reader still sees what the reopen is waiting on.
			w.run.Commit(w.state)
		} else {
			w.commit(ReasonSyncFailed, err.Error())
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// establish brings one kind up to date and streams it. A cookie means a completed LIST is on
// disk, so the watch resumes from it; without one the collection is cold-listed first.
func (w *kindSync) establish(ctx context.Context) error {
	k := w.run.Kind
	cookie, resuming, err := w.run.Store.Cookie(ctx, k.APIVersion, k.Resource)
	if err != nil {
		return err
	}

	if resuming && !w.relist {
		// A resume holds its reason: this kind has its rows, and walking every one of them
		// through Syncing on a resume poke is what makes a laptop opening cost a reconcile
		// per kind. Only a resume slow enough to have stopped being current says so, which
		// openWatch announces.
		//
		// Pruned on the way in, since the delta cadence only counts within one run: a mirror
		// restarted more often than the cadence comes round would resume forever without ever
		// reaching it, and a resume never takes the list that prunes.
		if err := w.pruneEventsNow(ctx); err != nil {
			return err
		}
		return w.stream(ctx, cookie, true)
	}
	// Both starts below take a full list, and they are not the same news: a cache holding
	// rows serves them throughout, where a cold one has nothing. The relist is what clears
	// the cookie, so a run that dies mid-page leaves none standing and the next one
	// reconciles rather than resuming over the gap.
	reason := Reason(ReasonSyncing)
	if w.relist {
		reason = ReasonResyncing
	}
	w.commit(reason, "")
	if cookie, err = w.coldList(ctx); err != nil {
		return err
	}
	// Only now: the cookie survives a list that failed before its first page, so a flag
	// dropped any earlier would send the next attempt back to a position the server has
	// already refused, and the run would alternate between a doomed watch and this list.
	w.relist = false
	return w.stream(ctx, cookie, false)
}

// coldList replaces this kind's rows from a paginated LIST and returns the position a watch
// resumes from. Behind the gate, which is what stops an armed cache putting hundreds of
// relists on the connection and the store's single writer at once.
func (w *kindSync) coldList(ctx context.Context) (string, error) {
	select {
	case w.pacing.coldLists <- struct{}{}:
		defer func() { <-w.pacing.coldLists }()
	case <-ctx.Done():
		return "", ctx.Err()
	}

	replace, err := w.run.Store.BeginReplace(w.run.Kind)
	if err != nil {
		return "", err
	}

	var cookie, next string
	for {
		page, err := w.collection().List(ctx, metav1.ListOptions{Limit: w.pacing.pageSize, Continue: next})
		if err != nil {
			return "", fmt.Errorf("list %s: %w", w.run.Kind.Resource, err)
		}
		items := make([]*unstructured.Unstructured, len(page.Items))
		for i := range page.Items {
			items[i] = &page.Items[i]
		}
		if err := replace.WritePage(ctx, items); err != nil {
			return "", err
		}
		// Every page of one LIST answers at the same position, so the last page's is the
		// snapshot's — and a later page is what a partial pass would be missing.
		cookie, next = page.GetResourceVersion(), page.GetContinue()
		if next == "" {
			break
		}
	}
	// Before the commit, which is what persists the cookie: the list can carry more than the
	// window and on an idle cluster no delta ever comes round to the cadence, so a prune that
	// failed after this collection became resumable would leave it oversized indefinitely.
	if err := w.pruneEventsNow(ctx); err != nil {
		return "", err
	}
	if _, err := replace.Commit(ctx, cookie); err != nil {
		return "", err
	}
	return cookie, nil
}

// openWatch establishes the stream, saying Resuming if a resume drags past the window its rows
// stay current for.
//
// The announcement is made from this goroutine, never a timer callback: one worker's state has
// one writer, and a callback firing as the stream settles would leave Resuming standing over
// the Watching it raced — Stop does not wait for one already running.
func (w *kindSync) openWatch(ctx context.Context, cookie string, announceSlow bool) (watch.Interface, error) {
	options := metav1.ListOptions{
		ResourceVersion: cookie,
		// Bookmarks are how an idle collection proves its watch is alive rather than wedged.
		AllowWatchBookmarks: true,
	}
	if !announceSlow {
		return w.collection().Watch(ctx, options)
	}

	// Buffered, so the open never blocks on a caller that has stopped waiting.
	done := make(chan opened, 1)
	go func() {
		stream, err := w.collection().Watch(ctx, options)
		done <- opened{stream, err}
	}()

	select {
	case o := <-done:
		return o.stream, o.err
	case <-ctx.Done():
		return nil, abandon(ctx, done)
	case <-time.After(w.pacing.staleAfter):
		w.commit(ReasonResuming, "")
	}

	// Still on ctx after the announcement: forgetting a kind waits for its worker, and a
	// worker parked on an open that will not unwind is one nothing can join.
	select {
	case o := <-done:
		return o.stream, o.err
	case <-ctx.Done():
		return nil, abandon(ctx, done)
	}
}

// opened is what one watch open answered with.
type opened struct {
	stream watch.Interface
	err    error
}

// abandon leaves an open that outlived its run to finish into nothing. It has to be collected:
// a watch that lands after the worker is gone holds a connection nobody will close.
func abandon(ctx context.Context, done <-chan opened) error {
	go func() {
		if o := <-done; o.stream != nil {
			o.stream.Stop()
		}
	}()
	return ctx.Err()
}

// stream applies deltas until the watch ends or the run does. Establishing it is what settles
// the verdict: the rows are current from here until the stream stops proving itself.
func (w *kindSync) stream(ctx context.Context, cookie string, announceSlow bool) error {
	stream, err := w.openWatch(ctx, cookie, announceSlow)
	if err != nil {
		return fmt.Errorf("watch %s: %w", w.run.Kind.Resource, err)
	}
	defer stream.Stop()

	// The verdict settles on the open; the retry streak does not. An open the server closes
	// without a frame has proven nothing, and clearing it here would hold such a run at the
	// base delay forever — live() clears it on the first frame instead.
	w.commit(ReasonWatching, "")

	stale := time.NewTimer(w.pacing.staleAfter)
	defer stale.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stale.C:
			// Nothing has proven the stream alive for a whole window. The rows are still
			// served — they are simply no longer known to be current.
			w.commit(ReasonStale, "")
		case event, ok := <-stream.ResultChan():
			if !ok {
				return errWatchClosed
			}
			if err := w.apply(ctx, event); err != nil {
				return err
			}
			stale.Reset(w.pacing.staleAfter)
		}
	}
}

// apply lands one watch event. A bookmark carries no object of ours — it exists to move the
// position and prove the stream alive — so it books progress and nothing else.
func (w *kindSync) apply(ctx context.Context, event watch.Event) error {
	if event.Type == watch.Error {
		// Wrapped, not flattened: whether the server dropped the position this stream reads
		// from is what decides between resuming and relisting.
		return fmt.Errorf("watch %s: %w", w.run.Kind.Resource, apierrors.FromObject(event.Object))
	}
	object, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("watch %s: unexpected %T", w.run.Kind.Resource, event.Object)
	}

	k := w.run.Kind
	if event.Type == watch.Bookmark {
		if err := w.run.Store.SetCookie(ctx, k.APIVersion, k.Resource, object.GetResourceVersion()); err != nil {
			return err
		}
		w.live()
		w.commit(ReasonWatching, "")
		return nil
	}

	// The row and the position that would replay it land together, so no restart resumes
	// from a position the rows do not back.
	if err := w.run.Store.ApplyChange(ctx, k, event.Type, object); err != nil {
		return err
	}
	w.state.LastUpdateAt = time.Now()
	w.live()
	w.commit(ReasonWatching, "")
	return w.pruneEvents(ctx)
}

// pruneEvents caps the events table. Events are the one collection that ages out rather than
// being deleted by the server, so nothing else would ever emit the departure a reader needs.
func (w *kindSync) pruneEvents(ctx context.Context) error {
	if !isCoreEvents(w.run.Kind) {
		return nil
	}
	if w.sincePrune++; w.sincePrune < w.pacing.eventsEvery {
		return nil
	}
	return w.pruneEventsNow(ctx)
}

// pruneEventsNow caps the table without waiting for the cadence.
func (w *kindSync) pruneEventsNow(ctx context.Context) error {
	if !isCoreEvents(w.run.Kind) {
		return nil
	}
	w.sincePrune = 0
	_, err := w.run.Store.PruneEvents(ctx, w.pacing.eventsWindow)
	return err
}

// live records proof this stream is current — a delta or a bookmark, since bookmarks exist to
// make an idle watch prove itself. A frame is also what ends a retry streak: nothing before one
// distinguishes a watch that works from a watch the server accepts and drops.
func (w *kindSync) live() {
	w.state.LastLiveAt = time.Now()
	w.state.Restarts, w.state.NextRetryAt = 0, time.Time{}
}

// commit publishes this worker's answer.
func (w *kindSync) commit(reason Reason, message string) {
	w.state.setReason(reason, message)
	w.run.Commit(w.state)
}

// collection is this kind's rows across every namespace: a mirror is the whole collection,
// and the cluster-scoped case is the same call without a namespace.
func (w *kindSync) collection() dynamic.ResourceInterface {
	return w.run.Conn.Dynamic.Resource(gvrOf(w.run.Kind))
}

func gvrOf(k kubestore.Kind) schema.GroupVersionResource {
	gv, err := schema.ParseGroupVersion(k.APIVersion)
	if err != nil {
		// A kind reaches here from kind_catalog, whose rows are group-versions the server
		// itself named, so this is unparseable only if the table is corrupt.
		return schema.GroupVersionResource{Resource: k.Resource}
	}
	return gv.WithResource(k.Resource)
}

// isCoreEvents asks by api version and plural, never by the Kind name: any group may serve a
// Kind called Event, and a CRD's rows are ordinary objects.
func isCoreEvents(k kubestore.Kind) bool {
	return k.APIVersion == "v1" && k.Resource == "events"
}
