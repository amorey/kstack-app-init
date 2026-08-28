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

// One kind's sync, as a reconciler on the supervisor. A run makes sure the kind's stream is up —
// the gate, then a cold LIST or a resume from the cookie, then the WATCH — and commits the stream
// as its value; the goroutine that applies deltas outlives it. Every way the stream ends is a
// Wake, which is how its end reaches the schedule.
//
// → docs/adr/2026-08-28-the-stream-is-the-value.md.
package kubesync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/supervisor"
)

// errWatchClosed is the apiserver ending a watch on its own timeout: the ordinary end of a
// stream rather than a failure of one.
var errWatchClosed = errors.New("watch closed")

// kindSyncInterval is how often a standing stream is re-checked for liveness. Long, because the
// stream's own exit is the real re-entry — this is only the backstop.
const kindSyncInterval = 10 * time.Minute

// nameKindSync is the kind reconciler's registration name on the kind supervisor. It is the only
// reconciler there: a kind is a subject, not a registration.
const nameKindSync = "sync"

// reasonWatchRotated has one job: to be a Failed attempt the overlay does not report. A rotation
// is paced like a failure — the reopen climbs a rung — but it is not news, since the rows stay
// current across it and saying so would flicker every kind through SyncFailed every few minutes.
const reasonWatchRotated Reason = "WatchRotated"

// kindSubject names one kind on one cache, "<cacheID>/<apiVersion>/<resource>", and
// parseKindSubject reads it back. apiVersion carries its own / for a group, so the name is parsed
// as first segment, last segment, and everything between.
func kindSubject(cacheID int64, k kubestore.Kind) string {
	return fmt.Sprintf("%d/%s/%s", cacheID, k.APIVersion, k.Resource)
}

func parseKindSubject(subject string) (int64, kindID, bool) {
	cachePart, rest, ok := strings.Cut(subject, "/")
	if !ok {
		return 0, kindID{}, false
	}
	i := strings.LastIndex(rest, "/")
	if i < 0 {
		return 0, kindID{}, false
	}
	cacheID, err := strconv.ParseInt(cachePart, 10, 64)
	if err != nil {
		return 0, kindID{}, false
	}
	return cacheID, kindID{apiVersion: rest[:i], resource: rest[i+1:]}, true
}

// kindStream is the handle to one standing stream: the goroutine applying deltas, and what a run
// reads to judge it.
//
// One ordering rule: the goroutine writes err, then closes done; a run reads done, then err.
// close(done) is the only happens-before edge, so nothing reads err while done is open.
type kindStream struct {
	cancel context.CancelFunc
	done   chan struct{}
	// err is how the stream ended: nil for a clean stop (a restart, a retirement, a Remove),
	// errWatchClosed for a rotation, anything else a failure.
	err error
	// proven is set by the goroutine on the first frame, while a run may be reading it. It is
	// what lets a run record the plain Succeeded that ends the retry streak.
	proven atomic.Bool
	// deathRecorded marks a dead stream whose failure a run has already recorded. Written and
	// read only by this subject's runs, which the supervisor serializes.
	deathRecorded bool
}

// alive reports whether the stream is still up.
func (st *kindStream) alive() bool {
	select {
	case <-st.done:
		return false
	default:
		return true
	}
}

// stop ends the stream and waits for its goroutine. Safe to call more than once.
func (st *kindStream) stop() {
	st.cancel()
	<-st.done
}

// pacing is every duration and bound a kind sync runs on. A test shrinks them; production
// passes defaultPacing, so no test outwaits a production number.
type pacing struct {
	// staleAfter is how long a watch may go without proving itself alive before its verdict
	// reads Stale. A resume that outlasts it says Resuming for the same reason: both are
	// "this stream has stopped being current".
	staleAfter time.Duration
	// backoff is the ladder a failing run climbs — the supervisor's, so a kind's retry countdown
	// reads the same as a sweep's.
	backoff supervisor.Backoff
	// pageSize bounds one relist page, so a large collection never lands in memory whole.
	pageSize int64
	// eventsWindow is how many event rows the cache keeps, and eventsEvery how many arrive
	// between prunes: the statement scans the table, so paying it per delta would make a
	// busy cluster's event stream quadratic.
	eventsWindow int
	eventsEvery  int
	// kindSyncWorkers bounds the kind runs in flight across every cache, and so bounds the
	// relists: a run holds a worker only through establishment, so the cap IS the cold-list
	// gate that arming a cache of hundreds of kinds needs.
	kindSyncWorkers int
}

func defaultPacing() pacing {
	return pacing{
		staleAfter:      5 * time.Minute,
		backoff:         supervisor.Backoff{Base: time.Second, Factor: 2, Cap: time.Minute},
		pageSize:        500,
		eventsWindow:    5000,
		eventsEvery:     100,
		kindSyncWorkers: 8,
	}
}

// kindReconciler is the reconciler every kind subject runs. It holds nothing per kind: the
// subject names one, and enterKindRun hands the run the whole value.
type kindReconciler struct {
	s      *Service
	pacing pacing
}

// Reconcile makes sure one kind's stream is up. It is short by construction: the gate, one
// establishment, and a return. What it starts outlives it as the committed value.
func (r kindReconciler) Reconcile(ctx context.Context, pass *supervisor.Pass[*kindStream]) supervisor.Result {
	cacheID, id, ok := parseKindSubject(pass.Subject())
	if !ok {
		return supervisor.Skip()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sess, k, run, ok := r.s.enterKindRun(cacheID, id, cancel)
	if !ok {
		// Nothing has armed this cache, its teardown has begun, or nothing tracks this kind:
		// either way the claims a run would write through are going.
		return supervisor.Skip()
	}
	defer r.s.leaveKindRun(sess, run)
	defer context.AfterFunc(sess.ctx, cancel)()

	if res, decided := standingVerdict(pass.Prev()); decided {
		return res
	}
	return r.establish(ctx, sess, k, run, pass)
}

// standingVerdict answers for the stream a subject already holds, in every case that starts
// nothing. A live stream is the whole answer. A dead one costs exactly one rung, recorded by the
// run its exit woke; the run the ladder then schedules finds the death recorded and
// re-establishes. Re-establishing on the same wake would make a server that closes watches on
// open a hot loop, and failing without ever starting would fail forever.
func standingVerdict(prev *kindStream) (supervisor.Result, bool) {
	switch {
	case prev == nil:
		return supervisor.Result{}, false
	case prev.alive():
		if prev.proven.Load() {
			return supervisor.Succeeded(), true
		}
		return supervisor.Succeeded().Provisional(), true
	case prev.err == nil || prev.deathRecorded:
		// A clean stop, or a death already paid for: re-establish now.
		return supervisor.Result{}, false
	}

	prev.deathRecorded = true
	if errors.Is(prev.err, errWatchClosed) {
		return supervisor.Fail(reasonWatchRotated, prev.err), true
	}
	return supervisor.Fail(ReasonSyncFailed, prev.err), true
}

// establish brings the stream up and commits it. Everything that can refuse is answered here:
// the identity gate suspends, and a list or an open that fails climbs the ladder.
func (r kindReconciler) establish(ctx context.Context, sess *session, k kubestore.Kind, run *kindRun, pass *supervisor.Pass[*kindStream]) supervisor.Result {
	conn, err := sess.lease.ConnFor(ctx, sess.params.ServerUID)
	if err != nil {
		// Nothing syncs into a cache whose connection does not vouch for its ServerUID. A run
		// holds a supervisor worker, so it records why and parks; the session's connection
		// bridge is what brings it back.
		return supervisor.Suspend(connectionReason(err, ReasonSyncFailed), err.Error())
	}
	// The run ends with the connection it dialed: a cold list against a retired one is a request
	// nothing will answer, and the retry goes through the gate to find the replacement.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-conn.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	prevState, _ := sess.kindState(idOf(k))
	syncer := &kindSyncer{
		kind:    k,
		conn:    conn,
		store:   sess.store,
		pacing:  r.pacing,
		publish: func(state KindState) { r.s.commitKind(sess, idOf(k), state) },
		state:   prevState,
	}
	// The one error a resume cannot retry through: the position the cookie names is one the
	// server has dropped, so the next start relists rather than resumes. A dead stream is one
	// way to learn it — safe to read here, since this runs only once the stream is down.
	id := idOf(k)
	if prev := pass.Prev(); prev != nil && positionGone(prev.err) {
		sess.markRelist(id)
	}
	syncer.relist = sess.needsRelist(id)

	// **The watch is opened on the stream's context, never the run's.** A watch is served over
	// the context its request was made on and dies with it, so one opened here on a run-scoped
	// context would be killed the moment the run returned — every established stream rotating
	// at once. The run still owns the cancel while it establishes, so a cold list ends with the
	// run; ownership passes to the stream below.
	streamCtx, streamCancel := context.WithCancel(sess.ctx)
	endWithRun := context.AfterFunc(ctx, streamCancel)

	wasRelist := syncer.relist
	watcher, err := syncer.open(streamCtx)
	if wasRelist && !syncer.relist {
		// The cold list landed, so the cookie behind it is this run's own — whatever the watch
		// it was about to open goes on to do.
		sess.clearRelist(id)
	}
	if err != nil {
		// **The other way to learn it, and the one nothing else carries.** A 410 answered by
		// the watch open establishes nothing, so this run commits nothing and leaves Prev
		// exactly as it was — and the two ordinary ways in have no stream to read anyway: a
		// reopen after a clean stop, and a cold start off a cookie on disk. Recorded on the
		// session, which outlives the run and every failure between here and the relist.
		if positionGone(err) {
			sess.markRelist(id)
		}
		ended := ctx.Err() != nil || streamCtx.Err() != nil
		endWithRun()
		streamCancel()
		if ended {
			// The session went, or the connection was retired under the list. Neither is this
			// kind's failure to report.
			return supervisor.Skip()
		}
		return supervisor.Fail(ReasonSyncFailed, err)
	}

	subject := kindSubject(sess.cacheID, k)
	stream := &kindStream{cancel: streamCancel, done: make(chan struct{})}
	// The first frame proves the stream, and its wake is what lets a run record the plain
	// Succeeded that ends the streak. Once per stream.
	syncer.onFrame = func() {
		if stream.proven.CompareAndSwap(false, true) {
			r.s.kindSupervisor.Wake(subject, nameKindSync)
		}
	}

	// Counted from inside the run, which still holds the group: a teardown joins the stream and
	// not only the run that started it.
	sess.runs.Add(1)
	run.stream.Store(stream)
	go func() {
		defer sess.runs.Done()
		defer streamCancel()

		err := syncer.applyDeltas(streamCtx, watcher)
		// err before done, and done before the wake: close(done) is the edge that publishes it.
		stream.err = err
		close(stream.done)
		r.s.kindSupervisor.Wake(subject, nameKindSync)
	}()

	// Ownership passes here: past this the run's return no longer ends the stream, and what
	// stops it is Discard, a retirement, or the session.
	endWithRun()
	pass.Commit(stream)
	// Established, and nothing has proven it yet — so the verdict stands and the streak does too.
	return supervisor.Succeeded().Provisional()
}

// positionGone reports the one error a resume cannot retry its way out of: the cookie is what it
// would retry with, and the server no longer serves from there.
func positionGone(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// Discard is the supervisor handing a stream back — on Remove, on Close, or for a commit it
// refused — and nothing else can reach the goroutine to stop it. It joins, which is what makes
// ForgetKind and a teardown synchronous.
func (r kindReconciler) Discard(stream *kindStream) {
	if stream != nil {
		stream.stop()
	}
}

// kindSyncer is one establishment of one kind: the cold list or resume the run performs, then
// the delta loop the goroutine runs. Built fresh per establishment, so once the run has returned
// the goroutine owns it alone.
type kindSyncer struct {
	kind   kubestore.Kind
	conn   *kubeconn.Connection
	store  *kubestore.Store
	pacing pacing
	// publish hands one answer to the session, which is the one writer of what a reader sees.
	publish func(KindState)
	// onFrame is called on every frame that proves the stream live; the run that starts the
	// goroutine sets it.
	onFrame func()

	state KindState
	// sincePrune counts event deltas applied since the last prune.
	sincePrune int
	// relist forces this attempt to cold-list, for the one failure a resume cannot retry its
	// way out of: the position the cookie names is the position the server dropped.
	relist bool
}

// open brings one kind up to date and hands back the watch it is current at. A cookie means a
// completed LIST is on disk, so the watch resumes from it; without one the collection is
// cold-listed first.
func (ks *kindSyncer) open(ctx context.Context) (watch.Interface, error) {
	k := ks.kind
	cookie, resuming, err := ks.store.Cookie(ctx, k.APIVersion, k.Resource)
	if err != nil {
		return nil, err
	}

	if resuming && !ks.relist {
		// A resume holds its reason: this kind has its rows, and walking every one of them
		// through Syncing on a resume poke is what makes a laptop opening cost a reconcile
		// per kind. Only a resume slow enough to have stopped being current says so, which
		// openWatch announces.
		//
		// Pruned on the way in, since the delta cadence only counts within one stream: a kind
		// restarted more often than the cadence comes round would resume forever without ever
		// reaching it, and a resume never takes the list that prunes.
		if err := ks.pruneEventsNow(ctx); err != nil {
			return nil, err
		}
		return ks.openWatch(ctx, cookie, true)
	}
	// Both starts below take a full list, and they are not the same news: a cache holding
	// rows serves them throughout, where a cold one has nothing. The relist is what clears
	// the cookie, so a run that dies mid-page leaves none standing and the next one
	// reconciles rather than resuming over the gap.
	reason := Reason(ReasonSyncing)
	if ks.relist {
		reason = ReasonResyncing
	}
	ks.report(reason, "")
	if cookie, err = ks.coldList(ctx); err != nil {
		return nil, err
	}
	// Only now: the cookie survives a list that failed before its first page, so a flag
	// dropped any earlier would send the next attempt back to a position the server has
	// already refused, and the run would alternate between a doomed watch and this list.
	ks.relist = false
	return ks.openWatch(ctx, cookie, false)
}

// coldList replaces this kind's rows from a paginated LIST and returns the position a watch
// resumes from. The supervisor's worker cap is what stops an armed cache putting hundreds of
// these on the connection and the store's single writer at once.
func (ks *kindSyncer) coldList(ctx context.Context) (string, error) {
	replace, err := ks.store.BeginReplace(ks.kind)
	if err != nil {
		return "", err
	}

	var cookie, next string
	for {
		page, err := ks.collection().List(ctx, metav1.ListOptions{Limit: ks.pacing.pageSize, Continue: next})
		if err != nil {
			return "", fmt.Errorf("list %s: %w", ks.kind.Resource, err)
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
	if err := ks.pruneEventsNow(ctx); err != nil {
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
// The announcement is made from this goroutine, never a timer callback: one kind's state has
// one writer, and a callback firing as the stream settles would leave Resuming standing over
// the Watching it raced — Stop does not wait for one already running.
func (ks *kindSyncer) openWatch(ctx context.Context, cookie string, announceSlow bool) (watch.Interface, error) {
	options := metav1.ListOptions{
		ResourceVersion: cookie,
		// Bookmarks are how an idle collection proves its watch is alive rather than wedged.
		AllowWatchBookmarks: true,
	}
	if !announceSlow {
		return ks.collection().Watch(ctx, options)
	}

	// Buffered, so the open never blocks on a caller that has stopped waiting.
	done := make(chan opened, 1)
	go func() {
		watcher, err := ks.collection().Watch(ctx, options)
		done <- opened{watcher, err}
	}()

	select {
	case o := <-done:
		return o.watcher, o.err
	case <-ctx.Done():
		return nil, abandon(ctx, done)
	case <-time.After(ks.pacing.staleAfter):
		ks.report(ReasonResuming, "")
	}

	// Still on ctx after the announcement: forgetting a kind joins its run, and a run parked
	// on an open that will not unwind is one nothing can join.
	select {
	case o := <-done:
		return o.watcher, o.err
	case <-ctx.Done():
		return nil, abandon(ctx, done)
	}
}

// opened is what one watch open answered with.
type opened struct {
	watcher watch.Interface
	err     error
}

// abandon leaves an open that outlived its run to finish into nothing. It has to be collected:
// a watch that lands after the run is gone holds a connection nobody will close.
func abandon(ctx context.Context, done <-chan opened) error {
	go func() {
		if o := <-done; o.watcher != nil {
			o.watcher.Stop()
		}
	}()
	return ctx.Err()
}

// applyDeltas applies the watch's events until it ends or the session does. It runs on the
// stream's goroutine, outliving the run that opened the watch, and how it returns is the
// stream's err.
//
// The verdict settles on the open; the retry streak does not. An open the server closes without
// a frame has proven nothing, and clearing the streak here would hold such a run at the base
// delay forever — the first frame's wake is what clears it.
func (ks *kindSyncer) applyDeltas(ctx context.Context, watcher watch.Interface) error {
	defer watcher.Stop()

	ks.report(ReasonWatching, "")

	stale := time.NewTimer(ks.pacing.staleAfter)
	defer stale.Stop()

	for {
		select {
		case <-ctx.Done():
			// A clean stop: a restart, a retirement, or the session going. The run this wakes
			// re-establishes at once rather than climbing a rung.
			return nil
		case <-ks.conn.Done():
			// The pool retired what this stream reads over. Clean too: the replacement is what
			// the run it wakes goes through the gate to find.
			return nil
		case <-stale.C:
			// Nothing has proven the stream alive for a whole window. The rows are still
			// served — they are simply no longer known to be current.
			ks.report(ReasonStale, "")
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return errWatchClosed
			}
			if err := ks.apply(ctx, event); err != nil {
				return err
			}
			stale.Reset(ks.pacing.staleAfter)
		}
	}
}

// apply lands one watch event. A bookmark carries no object of ours — it exists to move the
// position and prove the stream alive — so it books progress and nothing else.
func (ks *kindSyncer) apply(ctx context.Context, event watch.Event) error {
	if event.Type == watch.Error {
		// Wrapped, not flattened: whether the server dropped the position this stream reads
		// from is what decides between resuming and relisting.
		return fmt.Errorf("watch %s: %w", ks.kind.Resource, apierrors.FromObject(event.Object))
	}
	object, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("watch %s: unexpected %T", ks.kind.Resource, event.Object)
	}

	k := ks.kind
	if event.Type == watch.Bookmark {
		if err := ks.store.SetCookie(ctx, k.APIVersion, k.Resource, object.GetResourceVersion()); err != nil {
			return err
		}
		ks.proveLive()
		ks.report(ReasonWatching, "")
		return nil
	}

	// The row and the position that would replay it land together, so no restart resumes
	// from a position the rows do not back.
	if err := ks.store.ApplyChange(ctx, k, event.Type, object); err != nil {
		return err
	}
	ks.state.LastUpdateAt = time.Now()
	ks.proveLive()
	ks.report(ReasonWatching, "")
	return ks.pruneEvents(ctx)
}

// pruneEvents caps the events table. Events are the one collection that ages out rather than
// being deleted by the server, so nothing else would ever emit the departure a reader needs.
func (ks *kindSyncer) pruneEvents(ctx context.Context) error {
	if !isCoreEvents(ks.kind) {
		return nil
	}
	if ks.sincePrune++; ks.sincePrune < ks.pacing.eventsEvery {
		return nil
	}
	return ks.pruneEventsNow(ctx)
}

// pruneEventsNow caps the table without waiting for the cadence.
func (ks *kindSyncer) pruneEventsNow(ctx context.Context) error {
	if !isCoreEvents(ks.kind) {
		return nil
	}
	ks.sincePrune = 0
	_, err := ks.store.PruneEvents(ctx, ks.pacing.eventsWindow)
	return err
}

// proveLive records proof this stream is current — a delta or a bookmark, since bookmarks exist
// to make an idle watch prove itself — and tells the run about it: nothing before a frame
// distinguishes a watch that works from one the server accepts and drops.
func (ks *kindSyncer) proveLive() {
	ks.state.LastLiveAt = time.Now()
	if ks.onFrame != nil {
		ks.onFrame()
	}
}

// report publishes this kind's answer under a reason.
func (ks *kindSyncer) report(reason Reason, message string) {
	ks.state.setReason(reason, message)
	ks.publish(ks.state)
}

// collection is this kind's rows across every namespace: a kind sync is the whole collection,
// and the cluster-scoped case is the same call without a namespace.
func (ks *kindSyncer) collection() dynamic.ResourceInterface {
	return ks.conn.Dynamic.Resource(gvrOf(ks.kind))
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
