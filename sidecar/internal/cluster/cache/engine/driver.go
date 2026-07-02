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

package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	toolswatch "k8s.io/client-go/tools/watch"
)

// One kindDriver per (cluster, GVK) replaces the raw client-go Reflector. It
// exists because the Reflector can't be seeded with a stored resourceVersion —
// its lastSyncResourceVersion is unexported and starts empty, so it always
// relists with RV="0", transferring every object body on every wake. The driver
// instead:
//
//  1. RESUMES from the kind's persisted resourceVersion via a RetryWatcher (the
//     right built-in for "watch from a known RV"): on a wake the server usually
//     still has that RV, so we apply only deltas and never LIST.
//  2. On 410 Gone (RV too old) or a cold cache, falls back to a METADATA-FIRST
//     full re-sync: list metadata only, diff by uid+resourceVersion, fetch full
//     bodies only for new/changed objects, delete the ones that vanished.
//
// RetryWatcher owns the watch-phase reconnect/backoff; the only explicit backoff
// here guards fullResync's list/get calls.

// defaultDiffThreshold is the changed-object count past which a single full LIST
// beats N individual GETs in the metadata-diff resync. A wake typically changes
// a handful of objects, so the diff path wins; a churned cluster (or an empty
// cache) takes the LIST.
const defaultDiffThreshold = 200

// errExpired means the watch's resourceVersion is too old (410 Gone); the only
// recovery is a full re-sync to obtain a fresh RV.
var errExpired = errors.New("clustersync: watch resourceVersion expired")

// objMeta is the minimal identity the metadata-diff needs from a metadata list.
type objMeta struct {
	UID             string
	Namespace       string
	Name            string
	ResourceVersion string
}

// kubeSource is the per-GVR upstream the driver pulls from. liveSource wraps the
// dynamic + metadata clients; tests supply a fake.
type kubeSource interface {
	// List returns full unstructured objects plus the list's resourceVersion.
	List(ctx context.Context, opts metav1.ListOptions) ([]*unstructured.Unstructured, string, error)
	// ListMetadata returns metadata-only identities plus the list's resourceVersion.
	ListMetadata(ctx context.Context, opts metav1.ListOptions) ([]objMeta, string, error)
	// Get fetches one full object (namespace "" = cluster-scoped).
	Get(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error)
	// Watch opens a watch; the caller (RetryWatcher) supplies RV + AllowWatchBookmarks in opts.
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

// kindStore is the store seam every driver writes through. Both objectsStore and
// eventsStore satisfy it (see store.go); the driver never touches SQL directly.
type kindStore interface {
	ApplyChange(t watch.EventType, u *unstructured.Unstructured) error
	ReplaceFull(items []*unstructured.Unstructured, rv string) error
	PersistRV(ctx context.Context, rv string) error
}

// metadataDiffStore is the optional extension for kinds that participate in the
// metadata-first diff resync. Only objectsStore implements it — events have no
// resource_version column and no delete-missing, so fullResync type-asserts this
// rather than carrying SnapshotRVs/DeleteByUID (and a capability bool) on every
// store.
type metadataDiffStore interface {
	kindStore
	SnapshotRVs(ctx context.Context) (map[string]string, error)
	DeleteByUID(ctx context.Context, uid string) error
}

var (
	_ kindStore         = (*objectsStore)(nil)
	_ kindStore         = (*eventsStore)(nil)
	_ metadataDiffStore = (*objectsStore)(nil)
)

// kindDriver owns the resume/full-resync state machine for one GVR.
type kindDriver struct {
	src   kubeSource
	store kindStore
	gvk   schema.GroupVersionKind

	seedRV string // persisted resume point at construction

	// onWatch, when set, is invoked once — the first time the driver enters
	// its watch phase. The engine counts these down to flip its reported
	// state from Syncing to Watching.
	onWatch       func()
	sawWatch      bool
	diffThreshold int
	backoffInit   time.Duration
	backoffMax    time.Duration
	// sleep is a ctx-aware backoff sleep; a test seam so backoff is deterministic.
	sleep func(ctx context.Context, d time.Duration) error
}

// option configures a kindDriver's test seams. Production never tunes them, so
// newKindDriver exposes none — mirroring the unexported-option pattern in
// internal/cloud/prefsync and internal/poke.
type option func(*kindDriver)

func withSleep(fn func(context.Context, time.Duration) error) option {
	return func(d *kindDriver) { d.sleep = fn }
}

func withDiffThreshold(n int) option {
	return func(d *kindDriver) { d.diffThreshold = n }
}

func newKindDriver(src kubeSource, store kindStore, gvk schema.GroupVersionKind, seedRV string) *kindDriver {
	return newKindDriverWithOptions(src, store, gvk, seedRV)
}

func newKindDriverWithOptions(src kubeSource, store kindStore, gvk schema.GroupVersionKind, seedRV string, opts ...option) *kindDriver {
	d := &kindDriver{
		src:           src,
		store:         store,
		gvk:           gvk,
		seedRV:        seedRV,
		diffThreshold: defaultDiffThreshold,
		backoffInit:   1 * time.Second,
		backoffMax:    30 * time.Second,
		sleep:         ctxSleep,
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Run blocks until ctx is cancelled, resuming from the seed RV when possible and
// falling back to a metadata-first full re-sync otherwise.
func (d *kindDriver) Run(ctx context.Context) error {
	rv := d.seedRV
	backoff := d.backoffInit
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// A full re-sync is needed when we have no resume point (cold cache or a
		// prior 410). RetryWatcher rejects RV "" / "0", so a re-sync that yields
		// no usable RV is treated as a failure and backs off like any other.
		if rv == "" || rv == "0" {
			newRV, err := d.fullResync(ctx)
			if err == nil && (newRV == "" || newRV == "0") {
				err = errors.New("clustersync: re-sync returned no usable resourceVersion")
			}
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Warn("clustersync: full re-sync failed, backing off", "gvk", d.gvk.String(), "err", err)
				if serr := d.sleep(ctx, backoff); serr != nil {
					return serr
				}
				backoff = d.nextBackoff(backoff)
				continue
			}
			rv = newRV
		}

		progressed, err := d.watchPhase(ctx, rv)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Resume ended; recompute via a re-sync. A watch that delivered events is
		// healthy — reset the backoff and re-sync immediately. A watch that ended
		// without progress is unrecoverable for this kind (e.g. list-but-not-watch
		// RBAC, or an aggregated API that rejects watch); back off so we don't
		// hot-loop full LISTs against the API server and SQLite.
		if progressed {
			backoff = d.backoffInit
		} else {
			if errors.Is(err, errExpired) {
				slog.Debug("clustersync: watch RV expired, full re-sync", "gvk", d.gvk.String())
			} else {
				slog.Warn("clustersync: watch ended without progress, backing off", "gvk", d.gvk.String())
			}
			if serr := d.sleep(ctx, backoff); serr != nil {
				return serr
			}
			backoff = d.nextBackoff(backoff)
		}
		rv = ""
	}
}

func (d *kindDriver) nextBackoff(b time.Duration) time.Duration {
	b *= 2
	if b > d.backoffMax {
		b = d.backoffMax
	}
	return b
}

// watchPhase resumes the watch from rv via a RetryWatcher and applies deltas
// until ctx cancellation, a 410 (errExpired), or the RetryWatcher giving up (nil
// — Run re-syncs either way). It reports whether the watch made progress (applied
// at least one delta), which Run uses to tell a healthy watch that dropped from
// one that never worked. RetryWatcher requests bookmarks and tracks RV across
// reconnects internally; it does not forward bookmark events, so we persist RV
// from object deltas only.
func (d *kindDriver) watchPhase(ctx context.Context, rv string) (progressed bool, err error) {
	rw, err := toolswatch.NewRetryWatcherWithContext(ctx, rv, watcherFor(d.src))
	if err != nil {
		// e.g. an unusable RV; let Run fall to a full re-sync.
		return false, err
	}
	defer rw.Stop()

	if !d.sawWatch {
		d.sawWatch = true
		if d.onWatch != nil {
			d.onWatch()
		}
	}

	sawExpired := false
	for {
		select {
		case <-ctx.Done():
			return progressed, ctx.Err()
		case ev, ok := <-rw.ResultChan():
			if !ok {
				if sawExpired {
					return progressed, errExpired
				}
				return progressed, nil
			}
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				u, ok := ev.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				if err := d.store.ApplyChange(ev.Type, u); err != nil {
					slog.Warn("clustersync: apply watch change", "gvk", d.gvk.String(), "err", err)
					continue
				}
				progressed = true
				if rvNow := u.GetResourceVersion(); rvNow != "" {
					if err := d.store.PersistRV(ctx, rvNow); err != nil {
						slog.Warn("clustersync: persist rv", "gvk", d.gvk.String(), "err", err)
					}
				}
			case watch.Error:
				// RetryWatcher forwards the Status then closes the channel. A 410
				// means our RV expired; anything else it treats as fatal too.
				if isExpired(ev.Object) {
					sawExpired = true
				}
			}
		}
	}
}

// fullResync reconciles the whole kind. For kinds that support the metadata diff
// (everything but events) it lists metadata only, fetches bodies for just the
// new/changed objects, and deletes the vanished ones — unless the changed set is
// large or the cache is empty, where one full LIST is cheaper. Events take the
// plain full-LIST path. Returns the resourceVersion to seed the watch from.
func (d *kindDriver) fullResync(ctx context.Context) (string, error) {
	md, ok := d.store.(metadataDiffStore)
	if !ok {
		return d.fullList(ctx) // events: no metadata diff
	}

	have, err := md.SnapshotRVs(ctx)
	if err != nil {
		return "", err
	}
	// Cold cache (initial sync): a full LIST is strictly cheaper than a metadata
	// list plus N GETs, so skip the metadata round trip entirely.
	if len(have) == 0 {
		return d.fullList(ctx)
	}

	metas, listRV, err := d.src.ListMetadata(ctx, metav1.ListOptions{})
	if err != nil {
		// Some aggregated APIs don't serve a metadata endpoint; fall back to a
		// full LIST rather than failing the kind.
		slog.Debug("clustersync: metadata list failed, full LIST", "gvk", d.gvk.String(), "err", err)
		return d.fullList(ctx)
	}

	var changed []objMeta
	seen := make(map[string]struct{}, len(metas))
	for _, m := range metas {
		seen[m.UID] = struct{}{}
		if have[m.UID] != m.ResourceVersion {
			changed = append(changed, m)
		}
	}

	// A big delta: one LIST beats N GETs.
	if len(changed) > d.diffThreshold {
		return d.fullList(ctx)
	}

	for _, m := range changed {
		u, err := d.src.Get(ctx, m.Namespace, m.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Raced a delete between the metadata list and the GET; the
				// missing-pass (next cycle) reconciles it.
				continue
			}
			return "", err
		}
		if err := d.store.ApplyChange(watch.Modified, u); err != nil {
			return "", err
		}
	}
	for uid := range have {
		if _, ok := seen[uid]; !ok {
			if err := md.DeleteByUID(ctx, uid); err != nil {
				return "", err
			}
		}
	}
	if err := d.store.PersistRV(ctx, listRV); err != nil {
		return "", err
	}
	return listRV, nil
}

func (d *kindDriver) fullList(ctx context.Context) (string, error) {
	items, rv, err := d.src.List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	if err := d.store.ReplaceFull(items, rv); err != nil {
		return "", err
	}
	return rv, nil
}

// isExpired reports whether a watch.Error event object is a 410 Gone /
// ResourceExpired status (our RV is too old to resume from).
func isExpired(obj runtime.Object) bool {
	err := apierrors.FromObject(obj)
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// watcherFor adapts a kubeSource to the cache.WatcherWithContext that
// NewRetryWatcherWithContext consumes (RetryWatcher fills in RV + bookmarks).
func watcherFor(src kubeSource) cache.WatcherWithContext {
	return watcherFunc(func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
		return src.Watch(ctx, opts)
	})
}

type watcherFunc func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)

func (f watcherFunc) WatchWithContext(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return f(ctx, opts)
}

// ctxSleep sleeps for d or until ctx is cancelled (returning ctx.Err()).
func ctxSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
