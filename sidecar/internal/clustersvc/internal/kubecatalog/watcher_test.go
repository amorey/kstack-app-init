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

package kubecatalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// openedWatch is one call on the watch endpoint: the collection it stood over, and the
// version it asked to resume from.
type openedWatch struct {
	gvr        schema.GroupVersionResource
	resumeFrom string
}

// fakeOpener stands in for the API server's watch endpoint: it records each open and hands
// back a stream the test drives.
type fakeOpener struct {
	opens *testutil.Probe[openedWatch]
	// streams is what each successive open returns, oldest first.
	streams chan *watch.FakeWatcher
	err     error
	// hold, when non-nil, parks every open until it closes — deliberately ignoring ctx, so a
	// stop cannot end the stream. That is what makes a watcher's teardown slow enough for a
	// test to run something inside it.
	hold chan struct{}
}

func newFakeOpener(t *testing.T) *fakeOpener {
	t.Helper()
	return &fakeOpener{
		opens:   testutil.NewProbe[openedWatch](8),
		streams: make(chan *watch.FakeWatcher, 8),
	}
}

// serve queues one stream for the next open and hands it back for the test to drive.
func (f *fakeOpener) serve() *watch.FakeWatcher {
	w := watch.NewFake()
	f.streams <- w
	return w
}

func (f *fakeOpener) open(_ context.Context, _ *kubeconn.Connection, gvr schema.GroupVersionResource, resumeFrom string) (watch.Interface, error) {
	f.opens.Fire(openedWatch{gvr: gvr, resumeFrom: resumeFrom})
	if f.hold != nil {
		<-f.hold
	}
	if f.err != nil {
		return nil, f.err
	}
	select {
	case w := <-f.streams:
		return w, nil
	default:
		return watch.NewFake(), nil
	}
}

// crdObject is one event payload, carrying the version the stream should resume from.
func crdObject(resourceVersion string) *unstructured.Unstructured {
	o := &unstructured.Unstructured{}
	o.SetName("widgets.example.com")
	o.SetResourceVersion(resourceVersion)
	return o
}

// runStream starts one of a watcher's streams over the fake and returns the wakes it fired.
// Unpaced, so a test about what an end does is not about how long the reopen waits.
func runStream(t *testing.T, f *fakeOpener) (*watcher, *testutil.Probe[struct{}], chan struct{}) {
	t.Helper()
	return runStreamPaced(t, f, 0)
}

// runStreamPaced is runStream with the reopen pacing a test picks — the production constant is
// never what a test waits out.
func runStreamPaced(t *testing.T, f *fakeOpener, reopenDelay time.Duration) (*watcher, *testutil.Probe[struct{}], chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	w := newWatcher(nil, cancel, 1, reopenDelay)
	wakes := testutil.NewProbe[struct{}](8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.stream(ctx, crdGVR, f.open, func() { wakes.Fire(struct{}{}) })
	}()
	return w, wakes, done
}

// Nothing else paces a reopen: a proxy or a stressed API server that hangs up on every stream
// as soon as it is opened would otherwise be reopened as fast as it can close. Nothing is lost
// in the wait — the reopen resumes from the version the stream last saw.
func TestStreamLoopPacesItsReopens(t *testing.T) {
	f := newFakeOpener(t)
	first := f.serve()
	// Far longer than the window below, so this is about the pacing existing rather than about
	// how precise it is.
	w, _, done := runStreamPaced(t, f, time.Hour)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	first.Action(watch.Bookmark, crdObject("42"))
	first.Stop()

	// A negative assertion has no event to wait for, so it needs a bounded window; an unpaced
	// loop reopens the instant the stream ends.
	testutil.NoRecv(t, f.opens.Chan(), 50*time.Millisecond, "an unpaced reopen")

	// The wait ends with the watcher, so a stop is never held up by it.
	w.stop()
	testutil.Wait(t, done, "the stream loop to end")
}

// A change to what the cluster serves is the whole point: the sweep has to run again, and
// the event's content is dropped because the sweep reads current state itself.
func TestStreamLoopWakesOnAChange(t *testing.T) {
	f := newFakeOpener(t)
	w := f.serve()
	_, wakes, _ := runStream(t, f)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	w.Modify(crdObject("7"))

	wakes.Await(t, "the wake a change owes")
}

// A bookmark carries no change — only the version that keeps the stream resumable. Waking on
// one would sweep on the bookmark cadence, which costs more than not watching at all.
func TestStreamLoopIgnoresABookmark(t *testing.T) {
	f := newFakeOpener(t)
	w := f.serve()
	_, wakes, _ := runStream(t, f)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	w.Action(watch.Bookmark, crdObject("7"))

	// A negative assertion has no event to wait for, so it needs a bounded window; the
	// loop handles an event immediately, so a short one is enough.
	testutil.NoRecv(t, wakes.Chan(), 50*time.Millisecond, "a wake for a bookmark")
}

// The whole reason continuation is in the design: a server-side timeout with nothing to
// report must cost a reopen, not a sweep. The version comes off the last event seen —
// a bookmark's, when nothing changed.
func TestStreamLoopResumesFromABookmarkWithoutWaking(t *testing.T) {
	f := newFakeOpener(t)
	first, second := f.serve(), f.serve()
	_, wakes, _ := runStream(t, f)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	first.Action(watch.Bookmark, crdObject("42"))
	first.Stop()

	assert.Equal(t, "42", f.opens.Await(t, "the reopen").resumeFrom, "resumed from the bookmark's version")
	testutil.NoRecv(t, wakes.Chan(), 50*time.Millisecond, "a wake for a clean end")
	second.Stop()
}

// A change's version is remembered too — the stream stays resumable whether or not the
// cluster is quiet, and the wake the change owes is separate from the resume.
func TestStreamLoopResumesFromAChange(t *testing.T) {
	f := newFakeOpener(t)
	first, second := f.serve(), f.serve()
	_, wakes, _ := runStream(t, f)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	first.Modify(crdObject("7"))
	wakes.Await(t, "the wake a change owes")
	first.Stop()

	assert.Equal(t, "7", f.opens.Await(t, "the reopen").resumeFrom)
	second.Stop()
}

// A server error that is not about our version proves nothing was missed, and re-listing over
// it loops: the sweep's answer to a dead watch is another watch, and a server erroring for its
// own reasons errors the replacement too — a full discovery pass per turn, paced by nothing but
// how fast the server can fail.
func TestStreamLoopEndsQuietlyOnAnErrorThatIsNotAboutOurVersion(t *testing.T) {
	f := newFakeOpener(t)
	stream := f.serve()
	w, wakes, done := runStream(t, f)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	stream.Action(watch.Bookmark, crdObject("42"))
	stream.Error(&metav1.Status{Reason: metav1.StatusReasonInternalError, Code: 500})

	testutil.Wait(t, done, "the stream loop to end")
	assert.True(t, w.spent())
	// A negative assertion has no event to wait for, so it needs a bounded window; the loop
	// wakes on the way out or not at all, and it has already returned.
	testutil.NoRecv(t, wakes.Chan(), 50*time.Millisecond, "a wake")
}

// The server refusing to serve from where we were is the one end that proves events were
// missed, and a re-list is the only answer to a gap of unknown size.
//
// **The watcher is spent before the wake goes out**, not after: the wake runs a sweep, and a
// sweep that read a spent watcher as one still standing would decline to replace it and leave
// the cluster on the poll for good.
func TestStreamLoopWakesWhenTheServerReportsAGap(t *testing.T) {
	f := newFakeOpener(t)
	stream := f.serve()
	w, wakes, done := runStream(t, f)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	stream.Action(watch.Bookmark, crdObject("42"))
	stream.Error(&metav1.Status{Reason: metav1.StatusReasonExpired, Code: 410})

	wakes.Await(t, "the wake a gap owes")
	assert.True(t, w.spent(), "spent before the wake, so the sweep it runs replaces it")
	testutil.Wait(t, done, "the stream loop to end")
}

// The ends that are not evidence of anything missed, only of a stream that cannot go on. They
// are silent, and the next sweep is what re-establishes.
//
// **Waking on one would loop**: the sweep's answer to a dead watch is to stand another one up,
// so a refusal that repeats — RBAC on a cluster-scoped collection repeats exactly — would buy
// a full discovery pass per turn, with no committed answer to make the spin visible.
func TestStreamLoopEndsQuietlyWhenItCannotContinue(t *testing.T) {
	tests := map[string]struct {
		// arrange runs before the loop starts, act after it is running. An opener that
		// refuses has to be set up first: the loop opens the moment it starts.
		arrange func(f *fakeOpener)
		act     func(f *fakeOpener)
	}{
		"ended before any event, so no version is known": {
			act: func(f *fakeOpener) { f.serve().Stop() },
		},
		"the open itself was refused": {
			arrange: func(f *fakeOpener) { f.err = errors.New("customresourcedefinitions is forbidden") },
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFakeOpener(t)
			if tt.arrange != nil {
				tt.arrange(f)
			}
			w, wakes, done := runStream(t, f)
			if tt.act != nil {
				tt.act(f)
			}

			testutil.Wait(t, done, "the stream loop to end")
			assert.True(t, w.spent(), "so the next sweep stands a watch up rather than reading this one as live")
			// A negative assertion has no event to wait for, so it needs a bounded window;
			// the loop wakes on the way out or not at all, and it has already returned.
			testutil.NoRecv(t, wakes.Chan(), 50*time.Millisecond, "a wake")
		})
	}
}

// Stopping is not a gap: the watcher is being torn down, or its connection replaced, and the
// sweep either is not wanted or will re-establish on its own.
func TestStreamLoopEndsQuietlyWhenStopped(t *testing.T) {
	f := newFakeOpener(t)
	f.serve()
	w, wakes, done := runStream(t, f)
	require.Equal(t, "", f.opens.Await(t, "the first open").resumeFrom)

	w.stop()

	testutil.Wait(t, done, "the stream loop to end")
	testutil.NoRecv(t, wakes.Chan(), 50*time.Millisecond, "a wake for a stop")
}

// **A refused open is spent before it is reported.** The sweep reads the watcher back the moment
// awaitOpen returns, and a refused watch that still read as standing would have the cluster
// scheduled at the healthy backstop with nothing watching it.
func TestARefusedOpenIsSpentBeforeItIsReported(t *testing.T) {
	f := newFakeOpener(t)
	f.err = assert.AnError
	w, _, done := runStream(t, f)

	w.awaitOpen(context.Background())

	assert.True(t, w.spent(), "a refused watch read as still standing")
	testutil.Wait(t, done, "the stream loop to end")
}

// A cluster's served kinds move for two reasons — a CRD, or an aggregated APIService — so
// the watcher stands over both. They resume independently: each collection has its own
// resource version, and one aging out says nothing about the other.
func TestWatcherWatchesBothCollections(t *testing.T) {
	f := newFakeOpener(t)
	w := startWatcher(context.Background(), nil, f.open, 0, func() {})
	t.Cleanup(w.stop)

	var watched []string
	for range 2 {
		watched = append(watched, f.opens.Await(t, "an open").gvr.Resource)
	}

	assert.ElementsMatch(t, []string{"customresourcedefinitions", "apiservices"}, watched)
}

// Stopping takes both streams with it: a watcher that left one behind would go on waking a
// subject nothing is sweeping for.
func TestWatcherStopEndsBothStreams(t *testing.T) {
	f := newFakeOpener(t)
	w := startWatcher(context.Background(), nil, f.open, 0, func() {})
	for range 2 {
		f.opens.Await(t, "an open")
	}

	w.stop()

	testutil.WaitReturn(t, w.stop, "a second stop to return")
}

// --- the production opener ---

// fakeDynamic is the API server behind the production opener: the collection's version comes off
// the list, and every watch records the version it asked to start from.
func fakeDynamic(t *testing.T, listVersion string) (*dynamicfake.FakeDynamicClient, chan string) {
	t.Helper()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"})

	client.PrependReactor("list", crdGVR.Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		list := &unstructured.UnstructuredList{}
		list.SetAPIVersion("apiextensions.k8s.io/v1")
		list.SetKind("CustomResourceDefinitionList")
		list.SetResourceVersion(listVersion)
		return true, list, nil
	})

	started := make(chan string, 1)
	client.PrependWatchReactor(crdGVR.Resource, func(a k8stesting.Action) (bool, watch.Interface, error) {
		started <- a.(k8stesting.WatchAction).GetWatchRestrictions().ResourceVersion
		return true, watch.NewFake(), nil
	})
	return client, started
}

// A watch opened with no version does not start where it is told to — it replays, streaming a
// synthetic Added for every object that already exists. Each one would wake the sweep that is
// about to read the same state, so a fresh watch costs a redundant discovery pass unless it is
// given a version to start at.
func TestOpenCollectionWatchStartsFromTheCollectionsVersion(t *testing.T) {
	client, started := fakeDynamic(t, "99")

	stream, err := openCollectionWatch(context.Background(), &kubeconn.Connection{Dynamic: client}, crdGVR, "")
	require.NoError(t, err)
	t.Cleanup(stream.Stop)

	assert.Equal(t, "99", testutil.Recv(t, started, "the version the watch started from"))
}

// A resuming stream already knows where it left off, so it costs no extra read.
func TestOpenCollectionWatchResumesWithoutReadingTheCollection(t *testing.T) {
	client, started := fakeDynamic(t, "99")

	stream, err := openCollectionWatch(context.Background(), &kubeconn.Connection{Dynamic: client}, crdGVR, "42")
	require.NoError(t, err)
	t.Cleanup(stream.Stop)

	assert.Equal(t, "42", testutil.Recv(t, started, "the version the watch started from"))
	for _, a := range client.Actions() {
		assert.NotEqual(t, "list", a.GetVerb(), "a resume reads nothing")
	}
}
