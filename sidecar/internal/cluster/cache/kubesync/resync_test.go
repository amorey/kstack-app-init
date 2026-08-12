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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// diffStore is a fakeStore that also participates in the metadata diff — the objectsync
// side of the capability. It tracks each row's resourceVersion so a test can model a warm
// cache that is partly out of date.
type diffStore struct {
	*fakeStore
	mu          sync.Mutex
	rvs         map[string]string
	deleted     []string
	deleteCalls int
}

func newDiffStore() *diffStore {
	return &diffStore{fakeStore: newFakeStore(), rvs: make(map[string]string)}
}

// seed puts a row in the store as if a previous pass had cached it at rv.
func (d *diffStore) seed(uid, rv string) {
	d.fakeStore.mu.Lock()
	d.fakeStore.rows[uid] = struct{}{}
	d.fakeStore.mu.Unlock()
	d.mu.Lock()
	d.rvs[uid] = rv
	d.mu.Unlock()
}

func (d *diffStore) SnapshotRVs(context.Context) (map[string]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]string, len(d.rvs))
	for uid, rv := range d.rvs {
		out[uid] = rv
	}
	return out, nil
}

func (d *diffStore) DeleteByUIDs(_ context.Context, uids []string) error {
	d.mu.Lock()
	d.deleted = append(d.deleted, uids...)
	for _, uid := range uids {
		delete(d.rvs, uid)
	}
	d.deleteCalls++
	d.mu.Unlock()
	d.fakeStore.mu.Lock()
	for _, uid := range uids {
		delete(d.fakeStore.rows, uid)
	}
	d.fakeStore.mu.Unlock()
	return nil
}

// ApplyDiff lands a fetched object WITHOUT advancing the cookie — the whole point of the
// diff's write path, so the fake records that distinction rather than papering over it.
func (d *diffStore) ApplyDiff(_ context.Context, u *unstructured.Unstructured) error {
	d.fakeStore.mu.Lock()
	d.fakeStore.rows[string(u.GetUID())] = struct{}{}
	d.fakeStore.mu.Unlock()
	d.mu.Lock()
	d.rvs[string(u.GetUID())] = u.GetResourceVersion()
	d.mu.Unlock()
	return nil
}

func (d *diffStore) ClearRV(context.Context) error {
	d.fakeStore.mu.Lock()
	defer d.fakeStore.mu.Unlock()
	d.fakeStore.rv = ""
	return nil
}

func (d *diffStore) deletedUIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.deleted...)
}

// newDiffDriver builds a driver directly (no worker/monitor) so resync can be called on
// its own — the strategy choice is what's under test, not the run loop.
func newDiffDriver(src Source, st Store, opts ...driverOption) *driver {
	return newDriver(src, st, "", "seed", opts...)
}

// The steady state: a warm cache, a quiet cluster. The whole point of the diff is that
// this costs one metadata list and NOTHING else — no bodies re-downloaded every resync
// period.
func TestResyncWarmCacheUnchangedFetchesNoBodies(t *testing.T) {
	st := newDiffStore()
	st.seed("uid-a", "10")
	st.seed("uid-b", "11")

	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			return []ObjectMeta{
				{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "10"},
				{UID: "uid-b", Namespace: "default", Name: "b", ResourceVersion: "11"},
			}, "", "42", nil
		},
	}

	rv, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "42", rv, "the watch must resume from the list's resourceVersion")
	assert.Equal(t, 1, src.metas())
	assert.Zero(t, src.gets(), "nothing moved, so no body should be fetched")
	assert.Zero(t, src.lists(), "the diff must not fall back to a full LIST")
	assert.Equal(t, "42", st.resumeRV(), "the cookie must reach the reconciled position")
}

// Only the objects whose resourceVersion moved are fetched, and only the ones the cluster
// no longer has are deleted.
func TestResyncFetchesOnlyChangedAndDeletesVanished(t *testing.T) {
	st := newDiffStore()
	st.seed("uid-same", "10")
	st.seed("uid-moved", "10")
	st.seed("uid-gone", "10")

	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			return []ObjectMeta{
				{UID: "uid-same", Namespace: "default", Name: "same", ResourceVersion: "10"},
				{UID: "uid-moved", Namespace: "default", Name: "moved", ResourceVersion: "11"},
				{UID: "uid-new", Namespace: "default", Name: "new", ResourceVersion: "12"},
			}, "", "50", nil
		},
		getFn: func(_, name string) (*unstructured.Unstructured, error) {
			switch name {
			case "moved":
				return newTestItem("uid-moved", "moved", "11"), nil
			case "new":
				return newTestItem("uid-new", "new", "12"), nil
			}
			return nil, fmt.Errorf("unexpected get for %q", name)
		},
	}

	_, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 2, src.gets(), "only the moved and the new object should be fetched")
	assert.Equal(t, []string{"uid-gone"}, st.deletedUIDs())
	assert.ElementsMatch(t, []string{"uid-same", "uid-moved", "uid-new"}, st.uids())
}

// The metadata list is paginated for the same reason the full LIST is: one page resident
// at a time, however large the collection. And the vanished set goes out in ONE batched
// delete — these land on the writer connection the whole cache shares.
func TestResyncPaginatesMetadataAndBatchesDeletes(t *testing.T) {
	st := newDiffStore()
	for _, uid := range []string{"uid-a", "uid-b", "uid-gone-1", "uid-gone-2"} {
		st.seed(uid, "10")
	}

	var pages int
	src := &fakeSource{
		metaFn: func(opts metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			pages++
			if opts.Continue == "" {
				return []ObjectMeta{
					{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "10"},
				}, "page-2", "60", nil
			}
			return []ObjectMeta{
				{UID: "uid-b", Namespace: "default", Name: "b", ResourceVersion: "10"},
			}, "", "60", nil
		},
	}

	rv, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "60", rv)
	assert.Equal(t, 2, pages, "the continue token must be followed")
	assert.Zero(t, src.gets(), "nothing moved across either page")
	assert.ElementsMatch(t, []string{"uid-gone-1", "uid-gone-2"}, st.deletedUIDs(),
		"an object on no page is gone from the cluster")
	assert.Equal(t, 1, st.deleteCalls, "the vanished set must go out in one batch")
}

// The threshold check fires mid-pagination: once enough has changed to lose to one LIST,
// there is no point paging the rest of the metadata to confirm it.
func TestResyncAbandonsPaginationOncePastTheThreshold(t *testing.T) {
	st := newDiffStore()
	for i := range 10 {
		st.seed(fmt.Sprintf("uid-%d", i), "old")
	}

	var pages int
	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			pages++
			return []ObjectMeta{
				{UID: "uid-0", Namespace: "default", Name: "uid-0", ResourceVersion: "new"},
				{UID: "uid-1", Namespace: "default", Name: "uid-1", ResourceVersion: "new"},
			}, "more", "60", nil
		},
		listFn: staticList("60", newTestItem("uid-0", "uid-0", "new")),
	}

	_, err := newDiffDriver(src, st, withDiffThreshold(1)).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, pages, "the pass must bail on the first page that crosses the threshold")
	assert.Equal(t, 1, src.lists())
}

// A resync that reports nothing is invisible to the two things that read the driver's
// pass state, and both failures are silent.
//
// The liveness stamps decide what the user is told: without them a kind whose watch
// connects but never streams ages into Stale and stays there, however many successful
// resyncs run underneath it. didResync/resyncItems drive the catch-up report: a pass that
// re-listed but left them unset is announced as a clean watch resume, and watchPhase then
// waits out the whole resumeGrace for a resume that never happened.
func TestResyncRecordsItsPass(t *testing.T) {
	t.Run("a pass that changed nothing is still a strong proof", func(t *testing.T) {
		st := newDiffStore()
		st.seed("uid-a", "10")
		src := &fakeSource{
			metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
				return []ObjectMeta{
					{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "10"},
				}, "", "42", nil
			},
		}
		d := newDiffDriver(src, st)
		before := d.liveness()

		_, err := d.resync(context.Background())
		require.NoError(t, err)

		live := d.liveness()
		assert.True(t, live.proof.After(before.proof),
			"a completed metadata list proves the watch's position is current")
		assert.True(t, live.write.IsZero(),
			"nothing landed, so 'last update received' must not tick")
		assert.True(t, d.didResync, "the pass must not be reported as a clean watch resume")
	})

	t.Run("a pass that applied rows counts as a write", func(t *testing.T) {
		st := newDiffStore()
		st.seed("uid-a", "10")
		src := &fakeSource{
			metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
				return []ObjectMeta{
					{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "11"},
				}, "", "50", nil
			},
			getFn: func(_, name string) (*unstructured.Unstructured, error) {
				return newTestItem("uid-a", name, "11"), nil
			},
		}
		d := newDiffDriver(src, st)

		_, err := d.resync(context.Background())
		require.NoError(t, err)

		live := d.liveness()
		assert.False(t, live.write.IsZero(), "an applied body is an update received")
		assert.Equal(t, 1, d.resyncItems)
	})

	t.Run("a pass that only deleted counts as a write", func(t *testing.T) {
		st := newDiffStore()
		st.seed("uid-gone", "10")
		src := &fakeSource{
			metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
				return nil, "", "50", nil
			},
		}
		d := newDiffDriver(src, st)

		_, err := d.resync(context.Background())
		require.NoError(t, err)

		assert.False(t, d.liveness().write.IsZero(), "a prune changed the cache")
	})
}

// The cookie must mean "a completed pass landed on disk" — the invariant the full LIST
// keeps by clearing on its first page. The diff has to keep it too: its objects arrive by
// GET in whatever order the metadata list named them, each carrying its own
// resourceVersion, so advancing the cookie per object would leave it AHEAD of changes the
// pass has not applied. A crash there would resume the next watch past them for good.
func TestResyncLeavesNoCookieUntilThePassCompletes(t *testing.T) {
	st := newDiffStore()
	st.seed("uid-a", "10")
	st.seed("uid-b", "10")
	require.NoError(t, st.PersistRV(context.Background(), "stale-cookie"))

	var seenMidPass string
	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			return []ObjectMeta{
				{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "99"},
				{UID: "uid-b", Namespace: "default", Name: "b", ResourceVersion: "98"},
			}, "", "50", nil
		},
		getFn: func(_, name string) (*unstructured.Unstructured, error) {
			// Sampled after the first object has landed: mid-pass there must be no cookie.
			if name == "b" {
				seenMidPass = st.resumeRV()
			}
			if name == "a" {
				return newTestItem("uid-a", "a", "99"), nil
			}
			return newTestItem("uid-b", "b", "98"), nil
		},
	}

	rv, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Empty(t, seenMidPass,
		"a pass in flight must leave no resume position — a crash here would skip the rest")
	assert.Equal(t, "50", rv)
	assert.Equal(t, "50", st.resumeRV(),
		"only a completed pass writes the cookie, and it writes the LIST's position")
}

// A metadata LIST that answers with no resourceVersion must not cost us the cookie we
// already have. Persist is an unconditional upsert and Run rejects ""/"0" as unusable, so
// writing one back would turn a warm resume into a cold full LIST of the whole collection
// on the next start — for a pass that changed nothing and therefore never cleared the
// cookie itself.
func TestResyncKeepsTheCookieWhenTheListReturnsNoRV(t *testing.T) {
	st := newDiffStore()
	st.seed("uid-a", "10")
	require.NoError(t, st.PersistRV(context.Background(), "good-cookie"))

	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			// Everything matches what we hold, so the pass writes nothing.
			return []ObjectMeta{
				{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "10"},
			}, "", "", nil
		},
	}

	rv, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Empty(t, rv, "the caller is told there is no usable position, and backs off")
	assert.Equal(t, "good-cookie", st.resumeRV(),
		"an unusable RV must not overwrite a position that still resumes")
}

// A cold cache would need every body anyway, so one LIST beats a metadata list plus a GET
// per object.
func TestResyncColdCacheUsesFullList(t *testing.T) {
	st := newDiffStore() // seeded with nothing
	src := &fakeSource{listFn: staticList("42", newTestItem("uid-a", "a", "10"))}

	rv, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "42", rv)
	assert.Equal(t, 1, src.lists())
	assert.Zero(t, src.metas(), "a cold cache must not pay for a metadata list first")
}

// Past the threshold a diff is strictly slower than one paginated LIST.
func TestResyncLargeDeltaFallsBackToFullList(t *testing.T) {
	st := newDiffStore()
	metas := make([]ObjectMeta, 0, 5)
	for i := range 5 {
		uid := fmt.Sprintf("uid-%d", i)
		st.seed(uid, "old")
		metas = append(metas, ObjectMeta{UID: uid, Namespace: "default", Name: uid, ResourceVersion: "new"})
	}

	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) { return metas, "", "50", nil },
		listFn: staticList("50", newTestItem("uid-0", "uid-0", "new")),
		getFn:  func(_, _ string) (*unstructured.Unstructured, error) { return nil, errors.New("should not GET") },
	}

	_, err := newDiffDriver(src, st, withDiffThreshold(3)).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, src.lists(), "past the threshold the pass must switch to one LIST")
	assert.Zero(t, src.gets())
}

// Some aggregated API servers serve no metadata endpoint. That must degrade to a full
// LIST, not fail the kind.
func TestResyncFallsBackWhenMetadataUnavailable(t *testing.T) {
	st := newDiffStore()
	st.seed("uid-a", "10")
	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			return nil, "", "", errors.New("the server could not find the requested resource")
		},
		listFn: staticList("42", newTestItem("uid-a", "a", "11")),
	}

	rv, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "42", rv)
	assert.Equal(t, 1, src.lists())
}

// The events store doesn't implement the capability — it has no per-object
// resourceVersion — so it must keep taking the full-LIST path.
func TestResyncPlainStoreUsesFullList(t *testing.T) {
	st := newFakeStore()
	src := &fakeSource{listFn: staticList("42", newTestItem("uid-a", "a", "10"))}

	_, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, src.lists())
	assert.Zero(t, src.metas(), "a store that can't report what it holds must not be asked")
}

// An object deleted between the metadata list and its GET must not fail the pass — and
// must not be deleted from the cache by this pass either, since it is still in the listed
// set. The next resync reconciles it.
func TestResyncToleratesDeleteRacingTheGet(t *testing.T) {
	st := newDiffStore()
	st.seed("uid-a", "10")

	src := &fakeSource{
		metaFn: func(metav1.ListOptions) ([]ObjectMeta, string, string, error) {
			return []ObjectMeta{
				{UID: "uid-a", Namespace: "default", Name: "a", ResourceVersion: "11"},
			}, "", "50", nil
		},
		getFn: func(_, name string) (*unstructured.Unstructured, error) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, name)
		},
	}

	rv, err := newDiffDriver(src, st).resync(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "50", rv)
	assert.Empty(t, st.deletedUIDs(), "an object still in the listed set must not be pruned")
}

// The catch-up report's facts describe the pass that got us here, and the controller
// renders them verbatim in the user-visible event log ("re-synced N objects in Xs"). Two
// things made them lie: the item count accumulated for the worker's whole life, and the
// fire can repeat — recordFailure re-arms it on every stuck crossing — so a bare reconnect
// long after the last LIST inherited that total and announced a re-sync that never ran.
// A cold pass that LISTS nothing is not a pass that did nothing: mark-and-sweep is how the
// full-LIST path deletes, so a collection the server has dropped entirely is emptied by the
// prune alone. Counting only what was listed made that a no-op, and "last update received"
// went on ageing from before the deletion — the diff path, which counts its vanished set,
// always got this right.
func TestFullListCountsWhatThePrunedRowsChanged(t *testing.T) {
	fs := newFakeStore()
	// A warm cache the server no longer has anything for.
	fs.rows["uid-gone-1"] = struct{}{}
	fs.rows["uid-gone-2"] = struct{}{}

	src := &fakeSource{listFn: staticList("100")}
	d := newDriver(src, fs, "", "seed")
	before := d.liveness()

	_, err := d.fullList(context.Background())
	require.NoError(t, err)

	require.Empty(t, fs.uids(), "the prune emptied the collection")
	live := d.liveness()
	assert.True(t, live.write.After(before.write),
		"deleting every row IS an update received; only the listed count said otherwise")
}

func TestCatchUpFactsDescribeThePassThatEarnedThem(t *testing.T) {
	type report struct {
		resynced bool
		items    int
	}
	var got []report
	d := newDriver(&fakeSource{}, newFakeStore(), "", "seed")
	d.onCaughtUp = func(resynced bool, items int) { got = append(got, report{resynced, items}) }

	// Two passes before anything is reported: the report is about a pass, not about every
	// pass ever run.
	d.recordPass(1000, 1000)
	d.recordPass(1000, 1000)
	d.fireCaughtUp()

	// A later recovery with no pass behind it — the watch simply came back.
	d.fireCaughtUp()

	assert.Equal(t, []report{{resynced: true, items: 1000}, {resynced: false, items: 0}}, got)
}

// A continue token can expire mid-pagination. The pass restarts from the top with a fresh
// session, and the half-written one is discarded wholesale — so an object that only the
// abandoned pass listed is pruned rather than surviving on the strength of a pass that
// never completed.
func TestFullListRestartsWhenTheContinueTokenExpires(t *testing.T) {
	st := newFakeStore()
	calls := 0
	src := &fakeSource{listFn: func(metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
		calls++
		switch calls {
		case 1:
			return []*unstructured.Unstructured{newTestItem("uid-a", "a", "1")}, "tok", "", nil
		case 2:
			return nil, "", "", apierrors.NewResourceExpired("continue token expired")
		default:
			return []*unstructured.Unstructured{newTestItem("uid-b", "b", "2")}, "", "9", nil
		}
	}}

	d := newTestDriver(src, st, "")
	rv, err := d.fullList(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "9", rv)
	assert.ElementsMatch(t, []string{"uid-b"}, st.uids(), "the abandoned session's page must not survive the prune")
}

// A collection too large to paginate within its continue token's lifetime gives up with
// errListRestartBudget rather than falling back to one unpaginated LIST, which would load
// every body at once — the exact blow-up pagination exists to avoid.
func TestFullListGivesUpAfterTooManyContinueTokenExpiries(t *testing.T) {
	st := newFakeStore()
	src := &fakeSource{listFn: func(opts metav1.ListOptions) ([]*unstructured.Unstructured, string, string, error) {
		if opts.Continue == "" {
			return []*unstructured.Unstructured{newTestItem("uid-a", "a", "1")}, "tok", "", nil
		}
		return nil, "", "", apierrors.NewResourceExpired("continue token expired")
	}}

	d := newTestDriver(src, st, "")
	_, err := d.fullList(context.Background())
	require.ErrorIs(t, err, errListRestartBudget)

	// Bounded, not a hot loop: one first page plus one expiring follow-up per attempt,
	// across the initial pass and maxListRestarts retries.
	assert.Equal(t, 2*(maxListRestarts+1), src.lists())
}
