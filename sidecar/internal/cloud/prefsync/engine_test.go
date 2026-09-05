package prefsync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
	"github.com/kubetail-org/kstack-app/sidecar/internal/poke"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fakeUpstream is a hand-written Upstream stub.
type fakeUpstream struct {
	snap      prefs.Settings
	snapErr   error
	watchCh   chan prefs.Settings
	watchErr  error
	updates   chan prefs.Settings
	updateErr error
}

func (f *fakeUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	return f.snap, f.snapErr
}

// Watch honors ctx (per the Upstream contract): it forwards watchCh onto a
// fresh channel that closes when watchCh closes OR ctx is cancelled, so Run's
// event loop exits on cancel instead of leaking a goroutine across tests.
func (f *fakeUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	if f.watchErr != nil {
		return nil, nil, f.watchErr
	}
	out := make(chan prefs.Settings)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-f.watchCh:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil, nil
}

func (f *fakeUpstream) Update(ctx context.Context, p prefs.Settings) (prefs.Settings, error) {
	if f.updates != nil {
		select {
		case f.updates <- p:
		case <-ctx.Done():
			return p, ctx.Err()
		}
	}
	return p, f.updateErr
}

func closedCh() chan prefs.Settings {
	ch := make(chan prefs.Settings)
	close(ch)
	return ch
}

// failDrainUpstream is a faithful upstream whose Watch channel closes when its
// context is cancelled (per the Upstream contract), with an always-failing
// Update — to exercise the drain-failure path under an otherwise-healthy watch.
type failDrainUpstream struct {
	updateErr error
}

func (f *failDrainUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	return prefs.Settings{}, nil
}

func (f *failDrainUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	ch := make(chan prefs.Settings)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil, nil
}

func (f *failDrainUpstream) Update(context.Context, prefs.Settings) (prefs.Settings, error) {
	return prefs.Settings{}, f.updateErr
}

// blockSnapshotUpstream blocks in Snapshot until its context is cancelled,
// signaling each entry on `entered` — to verify a Poke cancels in-flight
// upstream work (not just an already-open watch).
type blockSnapshotUpstream struct {
	entered *testutil.Probe[struct{}]
}

func newBlockSnapshotUpstream() *blockSnapshotUpstream {
	return &blockSnapshotUpstream{entered: testutil.NewProbe[struct{}](4)}
}

func (u *blockSnapshotUpstream) Snapshot(ctx context.Context) (prefs.Settings, error) {
	u.entered.Fire(struct{}{})
	<-ctx.Done()
	return prefs.Settings{}, ctx.Err()
}

func (u *blockSnapshotUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	ch := make(chan prefs.Settings)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil, nil
}

func (u *blockSnapshotUpstream) Update(context.Context, prefs.Settings) (prefs.Settings, error) {
	return prefs.Settings{}, nil
}

// echoUpstream models a cloud whose Watch emits its CURRENT state on subscribe
// and whose Update deep-merges the patch into that state. It lets a test verify
// that draining before opening the watch means the stream's initial event
// already reflects a pushed edit — so there's no stale pre-update event to
// clobber an acked local-first edit.
type echoUpstream struct {
	mu    sync.Mutex
	state prefs.Settings
}

func (u *echoUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.state, nil
}

func (u *echoUpstream) Update(_ context.Context, patch prefs.Settings) (prefs.Settings, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.state = prefs.Merge(u.state, patch)
	return u.state, nil
}

func (u *echoUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	u.mu.Lock()
	cur := u.state
	u.mu.Unlock()
	ch := make(chan prefs.Settings, 1)
	ch <- cur // on-subscribe: the current state (post-drain, includes pushed edits)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil, nil
}

// streamErrUpstream delivers one event then ends the stream with a terminal
// error (via the Watch error channel), to exercise the engine surfacing an SSE
// read/transport failure rather than treating it as a clean close.
type streamErrUpstream struct {
	err error
}

func (u *streamErrUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	return prefs.Settings{}, nil
}

func (u *streamErrUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	out := make(chan prefs.Settings)
	errCh := make(chan error, 1)
	go func() {
		select {
		case out <- prefs.Settings{Theme: new("x")}:
		case <-ctx.Done():
		}
		errCh <- u.err // buffered: publish terminal error before closing out
		close(out)
	}()
	return out, errCh, nil
}

func (u *streamErrUpstream) Update(context.Context, prefs.Settings) (prefs.Settings, error) {
	return prefs.Settings{}, nil
}

// A stream that ends with a terminal error (not a clean `complete`) is surfaced
// as a sync failure: the engine records it in Status.LastError and backs off,
// rather than treating the errored close as a healthy disconnect.
func TestStreamErrorIsSurfaced(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &streamErrUpstream{err: errors.New("connection reset")}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	runEngine(t, e)

	waitFor(t, func() bool {
		s := e.Status()
		return s.State == StateBackoff && s.LastError == "connection reset"
	}, "stream terminal error was not surfaced as a backoff with LastError")
}

// A Poke must cancel work already in flight (here, a blocked Snapshot), not just
// an open watch — so a sign-out racing the connect can't let an authenticated
// call complete. We prove it by observing the supervisor re-enter Snapshot: that
// can only happen if the first (blocked) call was cancelled.
func TestPokeCancelsInFlightSnapshot(t *testing.T) {
	store, q := newStoreQueue(t)
	up := newBlockSnapshotUpstream()
	e := newWithOptions(up, store, q, nil, testOpts()...)

	runEngine(t, e)

	up.entered.Await(t, "the engine to enter Snapshot") // and block there

	e.Poke() // must cancel the in-flight Snapshot

	// re-entered Snapshot ⇒ the blocked one was cancelled
	up.entered.Await(t, "Poke to cancel the in-flight Snapshot")
}

// runEngine starts e.Run in the background and registers cleanup that cancels
// it and WAITS for the goroutine to exit, so tests don't leak Run goroutines
// across repeated package runs (go test -count=N). Returns the run context.
func runEngine(t *testing.T, e *Engine) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return ctx
}

// testOpts parks the backoff sleep until ctx is cancelled, so the engine
// applies once and then idles deterministically (no spin, no wall-clock
// waits).
func testOpts() []option {
	return []option{
		withBackoff(time.Second, time.Minute),
		withNow(func() time.Time { return epoch }),
		withJitter(func(d time.Duration) time.Duration { return d }),
		withSleep(func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	}
}

// breakDir replaces dir with a regular file, so any atomicjson.Save targeting a
// path inside it fails: Save's os.MkdirAll(dir) errors because a path component
// is not a directory. This is the cross-platform way to inject a persist
// failure — chmod(0o500) doesn't restrict writes on Windows, and merely
// removing the dir doesn't work because Save's MkdirAll just recreates it.
func breakDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := os.WriteFile(dir, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func newStoreQueue(t *testing.T) (*prefs.Store, *mutationqueue.Queue) {
	t.Helper()
	dir := t.TempDir()
	store, err := prefs.NewStore(filepath.Join(dir, "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	q, err := mutationqueue.New(filepath.Join(dir, "queue.json"))
	if err != nil {
		t.Fatalf("queue New: %v", err)
	}
	return store, q
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: %s", msg)
		case <-tick.C:
		}
	}
}

// readUntil drains a settings subscription until pred holds or it times out.
func readUntil(t *testing.T, sub prefs.Subscription, pred func(prefs.Settings) bool) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case s := <-sub.Chan():
			if pred(s) {
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for expected settings")
		}
	}
}

// C27: on Run, a snapshot from the upstream is applied to the store and
// published to subscribers.
func TestSnapshotApplied(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &fakeUpstream{snap: prefs.Settings{Theme: new("dark")}, watchCh: closedCh()}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	sub := store.Subscribe()
	defer sub.Close()

	runEngine(t, e)

	readUntil(t, sub, func(s prefs.Settings) bool { return s.Theme != nil && *s.Theme == "dark" })
}

// C28: a delta arriving on the watch stream is applied.
func TestStreamDeltaApplied(t *testing.T) {
	store, q := newStoreQueue(t)
	ch := make(chan prefs.Settings, 1)
	up := &fakeUpstream{snap: prefs.Settings{Theme: new("base")}, watchCh: ch}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	sub := store.Subscribe()
	defer sub.Close()

	runEngine(t, e)

	ch <- prefs.Settings{Theme: new("blue")}
	readUntil(t, sub, func(s prefs.Settings) bool { return s.Theme != nil && *s.Theme == "blue" })
}

// C29: an upstream error enters the Offline waiting state with an escalating
// (exponential) backoff delay.
func TestUpstreamErrorBacksOff(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &fakeUpstream{snapErr: errors.New("boom")}

	sleeps := make(chan time.Duration)
	release := make(chan struct{})
	// Report the backoff delay, then PARK inside Sleep until the test releases
	// us. The real Sleep blocks for the whole delay — during which the engine
	// is genuinely Offline — so the fake must keep the engine parked too.
	// Returning right after the send would let Run loop back to StateConnecting
	// before the test reads Status(), racing the assertion below. Override the
	// parking sleep from testOpts by appending a withSleep last.
	opts := append(testOpts(), withSleep(func(ctx context.Context, d time.Duration) error {
		select {
		case sleeps <- d:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}))
	e := newWithOptions(up, store, q, nil, opts...)

	runEngine(t, e)

	// First error: engine is parked in Sleep, so the state is settled at Offline.
	d1 := <-sleeps
	if got := e.Status().State; got != StateOffline {
		t.Fatalf("state after error: want Offline, got %v", got)
	}
	release <- struct{}{} // let the first backoff finish → engine retries, fails again
	d2 := <-sleeps
	if d2 != 2*d1 {
		t.Fatalf("backoff did not escalate: d1=%v d2=%v", d1, d2)
	}
	// The engine is now parked in the second Sleep; cleanup's ctx cancel unparks
	// it (Sleep's ctx.Done case), so Run exits without another release.
}

// C30: a local Set writes the store immediately and enqueues the patch,
// without contacting the upstream (offline-capable).
func TestLocalSetWritesAndEnqueues(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &fakeUpstream{updates: make(chan prefs.Settings, 1)}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	if err := e.Set(prefs.Settings{Theme: new("dark")}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := store.Get(); got.Theme == nil || *got.Theme != "dark" {
		t.Fatalf("store after Set: %+v", got)
	}
	if pending := q.Pending(); len(pending) != 1 {
		t.Fatalf("queue after Set: want 1 pending, got %d", len(pending))
	}
	select {
	case <-up.updates:
		t.Fatal("upstream Update called for an offline Set")
	case <-time.After(100 * time.Millisecond):
		// good: no upstream contact
	}
}

// If the durable enqueue fails, Set must not expose the local change: the
// store stays at its prior value (nothing published) so an unqueued edit can't
// exist that the next snapshot would silently clobber.
func TestSetNotExposedWhenEnqueueFails(t *testing.T) {
	dir := t.TempDir()
	store, err := prefs.NewStore(filepath.Join(dir, "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	q, err := mutationqueue.New(filepath.Join(dir, "queue.json"))
	if err != nil {
		t.Fatalf("queue New: %v", err)
	}
	e := newWithOptions(&fakeUpstream{}, store, q, nil, testOpts()...)

	// Break the dir so the enqueue's atomic persist fails (cross-platform).
	breakDir(t, dir)

	if err := e.Set(prefs.Settings{Theme: new("dark")}); err == nil {
		t.Fatal("want error when enqueue fails, got nil")
	}
	if got := store.Get(); got.Theme != nil {
		t.Fatalf("store must be unchanged when enqueue fails, got %+v", got)
	}
	if n := len(q.Pending()); n != 0 {
		t.Fatalf("queue must be empty when enqueue fails, got %d", n)
	}
}

// If local persistence fails after the enqueue succeeds, Set rolls the queued
// mutation back, so a never-applied edit can't later be sent/acked to the cloud.
func TestSetRollsBackQueueWhenStoreFails(t *testing.T) {
	storeDir := t.TempDir()
	store, err := prefs.NewStore(filepath.Join(storeDir, "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	// Queue lives in its own (writable) dir so Enqueue succeeds while the store
	// write fails — exercising the rollback path, not a failed enqueue.
	q, err := mutationqueue.New(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("queue New: %v", err)
	}
	e := newWithOptions(&fakeUpstream{}, store, q, nil, testOpts()...)

	// Break only the store dir so Enqueue (separate dir) succeeds while the
	// store write fails — exercising the rollback path cross-platform.
	breakDir(t, storeDir)

	if err := e.Set(prefs.Settings{Theme: new("dark")}); err == nil {
		t.Fatal("want error when store write fails, got nil")
	}
	if n := len(q.Pending()); n != 0 {
		t.Fatalf("queued entry must be rolled back on store failure, got %d pending", n)
	}
}

// A snapshot that can't be persisted locally is a sync failure: the engine
// enters backoff (Offline) instead of marking it synced and opening the watch.
func TestSnapshotApplyFailureIsSyncFailure(t *testing.T) {
	storeDir := t.TempDir()
	store, err := prefs.NewStore(filepath.Join(storeDir, "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	q, err := mutationqueue.New(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("queue New: %v", err)
	}
	// Watch would block forever if reached — the test asserts we never get there.
	up := &fakeUpstream{snap: prefs.Settings{Theme: new("dark")}, watchCh: make(chan prefs.Settings)}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	// Break the store dir so persisting the snapshot fails (cross-platform).
	breakDir(t, storeDir)

	runEngine(t, e)

	waitFor(t, func() bool { return e.Status().State == StateOffline },
		"engine did not enter Offline on a failed snapshot apply")
	if !e.Status().LastSyncedAt.IsZero() {
		t.Fatal("LastSyncedAt stamped despite the snapshot never being persisted")
	}
}

// C31: on reaching Live the engine drains the queue via Upstream.Update and
// Acks each entry.
func TestQueueDrainedOnConnect(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &fakeUpstream{
		snap:    prefs.Settings{},
		watchCh: make(chan prefs.Settings), // stays open → engine reaches Live
		updates: make(chan prefs.Settings, 4),
	}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	// Enqueue an offline edit before the engine connects.
	if err := e.Set(prefs.Settings{Theme: new("dark")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	runEngine(t, e)

	if p := testutil.Recv(t, up.updates, "the queue drain Update"); p.Theme == nil || *p.Theme != "dark" {
		t.Fatalf("Update got %+v", p)
	}
	waitFor(t, func() bool { return len(q.Pending()) == 0 }, "queue not drained")
}

// When the queue's Ack can't be persisted, drain stops instead of treating
// already-sent patches as drained — otherwise a restart would replay them. The
// failed entry (and everything after it) stays durably queued.
func TestDrainStopsWhenAckFails(t *testing.T) {
	dir := t.TempDir()
	store, err := prefs.NewStore(filepath.Join(dir, "settings.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	q, err := mutationqueue.New(filepath.Join(dir, "queue.json"))
	if err != nil {
		t.Fatalf("queue New: %v", err)
	}
	up := &fakeUpstream{
		snap:    prefs.Settings{},
		watchCh: make(chan prefs.Settings),
		updates: make(chan prefs.Settings, 8),
	}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	// Two queued offline edits, enqueued while the dir still exists.
	if err := e.Set(prefs.Settings{Theme: new("a")}); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := e.Set(prefs.Settings{Theme: new("b")}); err != nil {
		t.Fatalf("Set b: %v", err)
	}

	// Break the data dir so the post-Update Ack persist fails (cross-platform).
	// The snapshot apply re-layers a,b → {Theme:b}, which already equals the
	// store's value, so its equality guard skips the disk write — only the Ack
	// actually touches the (now-broken) dir.
	breakDir(t, dir)

	runEngine(t, e)

	// Drain stops at the first entry whose Ack fails, so it never proceeds to
	// the second entry: every Update we observe (the engine retries 'a' across
	// backoff cycles) must be 'a', never 'b'.
	deadline := time.After(500 * time.Millisecond)
	updates := 0
	for done := false; !done; {
		select {
		case p := <-up.updates:
			if p.Theme == nil || *p.Theme != "a" {
				t.Fatalf("drain must stop at the failed entry 'a', but sent %+v", p)
			}
			updates++
		case <-deadline:
			done = true
		}
	}
	if updates == 0 {
		t.Fatal("expected at least one drain Update of 'a'")
	}
	// Both entries remain durably queued (nothing was acked).
	if n := len(q.Pending()); n != 2 {
		t.Fatalf("want 2 pending after failed Ack, got %d", n)
	}
}

// A queued local edit, once drained (pushed + acked), must not be clobbered by
// the watch stream's initial/on-subscribe event. Because the engine drains
// before opening the watch, that initial event already reflects the pushed edit
// — the local-first value survives even after the ack removes it from re-layering.
func TestDrainBeforeWatchProtectsAckedEdit(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &echoUpstream{state: prefs.Settings{Theme: new("cloud"), Locale: new("x")}}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	// A local-first edit: applied locally and queued before connecting.
	if err := e.Set(prefs.Settings{Theme: new("local")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	runEngine(t, e)

	// The queue drains (Theme=local pushed+acked), then the watch's on-subscribe
	// event already includes it, so the local edit is never reverted.
	waitFor(t, func() bool { return len(q.Pending()) == 0 }, "queue did not drain")
	waitFor(t, func() bool {
		got := store.Get()
		return got.Theme != nil && *got.Theme == "local" &&
			got.Locale != nil && *got.Locale == "x"
	}, "local edit was clobbered or the cloud field was not pulled")
}

// A drain failure backs the engine off (Offline) and leaves the entry queued,
// so it's retried on the next connection cycle rather than left unsynced.
func TestDrainFailureBacksOffAndRetries(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &failDrainUpstream{updateErr: errors.New("transient update failure")}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	// Queue a local edit for the engine to drain.
	if err := e.Set(prefs.Settings{Theme: new("dark")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	runEngine(t, e)

	waitFor(t, func() bool { return e.Status().State == StateOffline },
		"engine did not back off after a failed drain")
	// The failed entry stays durably queued for the next cycle.
	if n := len(q.Pending()); n != 1 {
		t.Fatalf("failed-drain entry must remain queued, got %d pending", n)
	}
}

// Overlapping Sets of disjoint fields must not lose an edit: the serialized
// read-merge-write means both fields survive even when the calls race. (Without
// the lock, both merge from the same base and the later write drops a field.)
func TestConcurrentSetsDoNotDropFields(t *testing.T) {
	store, q := newStoreQueue(t)
	e := newWithOptions(&fakeUpstream{}, store, q, nil, testOpts()...)

	for i := 0; i < 200; i++ {
		v := strconv.Itoa(i)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = e.Set(prefs.Settings{Theme: &v}) }()
		go func() { defer wg.Done(); _ = e.Set(prefs.Settings{Locale: &v}) }()
		wg.Wait()

		got := store.Get()
		if got.Theme == nil || *got.Theme != v || got.Locale == nil || *got.Locale != v {
			t.Fatalf("round %d lost a field: %+v", i, got)
		}
	}
}

// C32: a still-pending local patch is re-layered over an incoming snapshot
// (LWW deep-merge) — the snapshot's other fields apply, but the pending
// field is preserved until acked.
func TestPendingPatchMergedOverSnapshot(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &fakeUpstream{
		snap:      prefs.Settings{Theme: new("light"), Locale: new("en")},
		watchCh:   closedCh(),
		updateErr: errors.New("still offline"), // Update fails → patch stays pending
	}
	e := newWithOptions(up, store, q, nil, testOpts()...)

	// Local edit: Theme=dark, still pending.
	if err := e.Set(prefs.Settings{Theme: new("dark")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	sub := store.Subscribe()
	defer sub.Close()

	runEngine(t, e)

	// Snapshot brings Locale=en but must NOT clobber the pending Theme=dark.
	readUntil(t, sub, func(s prefs.Settings) bool {
		return s.Theme != nil && *s.Theme == "dark" && s.Locale != nil && *s.Locale == "en"
	})
}

// A poke (sign-out / forced resync) that cancels a pre-Live step is an
// intentional restart, not a sync failure: it must not be recorded as an error
// (which would surface a false LastError and briefly read Offline) nor ratchet
// backoff. We prove it by poking a blocked Snapshot and confirming the engine
// re-enters Snapshot with no LastError set.
func TestPokeDuringConnectIsNotAFailure(t *testing.T) {
	store, q := newStoreQueue(t)
	up := newBlockSnapshotUpstream()
	e := newWithOptions(up, store, q, nil, testOpts()...)

	runEngine(t, e)

	up.entered.Await(t, "the engine to enter Snapshot") // and block there

	e.Poke() // cancels the in-flight Snapshot — an intentional restart

	// re-entered Snapshot ⇒ the attempt was abandoned, not failed
	up.entered.Await(t, "the engine to reconnect after a poke-cancel")

	// Parked back in Snapshot: a poke-cancel must not have recorded a failure.
	if s := e.Status(); s.LastError != "" {
		t.Fatalf("poke-cancel recorded LastError=%q; an intentional cancel is not a sync failure", s.LastError)
	}
}

// cancelOnUpdateUpstream signals each Update and invokes onUpdate just before
// returning success — letting a test inject a cancellation into the window
// between a successful Update and its Ack.
type cancelOnUpdateUpstream struct {
	updated  *testutil.Signal
	onUpdate func()
}

func (u *cancelOnUpdateUpstream) Snapshot(context.Context) (prefs.Settings, error) {
	return prefs.Settings{}, nil
}

func (u *cancelOnUpdateUpstream) Watch(ctx context.Context) (<-chan prefs.Settings, <-chan error, error) {
	ch := make(chan prefs.Settings)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil, nil
}

func (u *cancelOnUpdateUpstream) Update(context.Context, prefs.Settings) (prefs.Settings, error) {
	u.updated.Fire()
	if u.onUpdate != nil {
		u.onUpdate()
	}
	return prefs.Settings{}, nil
}

// A cancellation landing after a queued patch is pushed (Update succeeds) but
// before its Ack must NOT ack the entry: it stays durably queued (the cloud's
// deep-merge makes a re-send next cycle idempotent) rather than being acked as
// the engine tears down. Without the post-Update ctx re-check the entry would
// be acked and lost from the queue.
func TestDrainDoesNotAckAfterCancel(t *testing.T) {
	store, q := newStoreQueue(t)
	ctx, cancel := context.WithCancel(context.Background())

	up := &cancelOnUpdateUpstream{updated: testutil.NewSignal()}
	// Cancel between Update succeeding and the Ack — the engine then exits.
	up.onUpdate = cancel
	e := newWithOptions(up, store, q, nil, testOpts()...)

	if err := e.Set(prefs.Settings{Theme: new("dark")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()

	up.updated.Wait(t, "the drain to push the queued patch")
	testutil.Wait(t, done, "the engine to exit after the context was cancelled")

	if n := len(q.Pending()); n != 1 {
		t.Fatalf("want the pushed-but-unacked patch still queued, got %d pending", n)
	}
}

// B6: a Signal received from the Notifier collapses the backoff and triggers
// an immediate reconnect.
func TestEngine_NotifierSignalPokes(t *testing.T) {
	store, q := newStoreQueue(t)
	// Upstream always fails Snapshot so the engine parks in backoff.
	up := newBlockSnapshotUpstream()

	b := poke.New()
	e := newWithOptions(up, store, q, b, testOpts()...)

	runEngine(t, e)

	// Wait for the first Snapshot to block (engine is in connecting state).
	up.entered.Await(t, "the engine to enter Snapshot")

	// Poke the broadcaster — the forwarding goroutine calls e.Poke(), which
	// cancels the in-flight Snapshot and wakes any in-flight backoff.
	b.Poke(poke.SourceHost)

	// Engine should re-enter Snapshot promptly (proving the poke propagated).
	up.entered.Await(t, "the notifier poke to trigger a reconnect")
}

// The state names ride the GraphQL sync-status field, so they are wire values,
// not debug output. An unnamed state reads as "unknown" rather than a number.
func TestStateStrings(t *testing.T) {
	for state, want := range map[State]string{
		StateConnecting: "connecting",
		StateLive:       "live",
		StateBackoff:    "backoff",
		StateOffline:    "offline",
		State(99):       "unknown",
	} {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
}

// The delay doubles per attempt and then holds at the cap — including once the
// shift has overflowed to a negative duration, which no cap comparison alone
// would catch.
func TestBackoffDelayCapsAtMax(t *testing.T) {
	store, q := newStoreQueue(t)
	e := newWithOptions(&fakeUpstream{}, store, q, nil, testOpts()...)

	if got := e.backoffDelay(0); got != time.Second {
		t.Errorf("backoffDelay(0) = %v, want 1s", got)
	}
	if got := e.backoffDelay(2); got != 4*time.Second {
		t.Errorf("backoffDelay(2) = %v, want 4s", got)
	}
	for _, attempt := range []int{10, 62, 64} {
		if got := e.backoffDelay(attempt); got != time.Minute {
			t.Errorf("backoffDelay(%d) = %v, want the 1m cap", attempt, got)
		}
	}
}

// The production seams, which every other test replaces: jitter picks a point in
// [d/2, d] and refuses to feed a non-positive duration to the RNG, and sleep
// returns once the delay is up.
func TestDefaultBackoffSeams(t *testing.T) {
	var o engineOpts
	o.applyDefaults()

	if got := o.jitter(0); got != 0 {
		t.Errorf("jitter(0) = %v, want 0", got)
	}
	for range 100 {
		got := o.jitter(time.Second)
		if got < time.Second/2 || got > time.Second {
			t.Fatalf("jitter(1s) = %v, want a point in [500ms, 1s]", got)
		}
	}
	if err := o.sleep(context.Background(), 0); err != nil {
		t.Errorf("sleep(0) = %v, want nil once the timer fires", err)
	}
}

// A watch that cannot be opened is a connect failure like any other: the engine
// backs off rather than treating the missing stream as a live one.
func TestWatchOpenFailureBacksOff(t *testing.T) {
	store, q := newStoreQueue(t)
	up := &fakeUpstream{watchErr: errors.New("stream refused")}

	sleeps := make(chan time.Duration)
	opts := append(testOpts(), withSleep(func(ctx context.Context, d time.Duration) error {
		select {
		case sleeps <- d:
		case <-ctx.Done():
		}
		<-ctx.Done()
		return ctx.Err()
	}))
	e := newWithOptions(up, store, q, nil, opts...)
	runEngine(t, e)

	testutil.Recv(t, sleeps, "the backoff after a refused watch")
	if got := e.Status(); got.State != StateOffline || got.LastError != "stream refused" {
		t.Fatalf("status = %+v, want Offline carrying the open error", got)
	}
}
