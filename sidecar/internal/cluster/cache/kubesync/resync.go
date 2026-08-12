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
	"log/slog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The resync pass is how the driver reaches a position it can watch from: a paginated full
// LIST, or — where the store can say what it already holds — a metadata diff that fetches
// only what moved. Both end by recording the pass, which refreshes the backstop deadline
// and the facts the catch-up report reads.

const (
	// listPageSize bounds one LIST (or diff-metadata) page so a whole collection never
	// sits in memory; maxListRestarts bounds continue-token-expiry restarts before the
	// pass is declared un-paginatable.
	listPageSize    = 500
	maxListRestarts = 3
)

// errListRestartBudget means the paginated LIST couldn't finish inside the continue
// token's lifetime. Returned rather than falling back to one unpaginated LIST — that
// would load every body at once, the exact blow-up pagination avoids.
var errListRestartBudget = errors.New("kubesync: continue token kept expiring; too many items to paginate within its lifetime")

// resync reconciles the whole collection and returns the resourceVersion to seed the next
// watch from. It runs when the watch can't be resumed — a cold cache, an expired cookie, or
// the periodic re-list that ends a long-lived watch on purpose.
//
// It picks between two strategies. Where the store can report what it already holds (a
// MetadataDiffStore) and the cache is warm, it lists IDENTITIES only, fetches bodies for
// just the objects whose resourceVersion moved, and deletes the ones that vanished — on a
// quiet cluster, which is the steady state, that is a small response and no body fetches at
// all, versus re-downloading every object every resyncPeriod. Otherwise it falls back to
// the paginated full LIST: a cold cache (where the diff would be one metadata list plus a
// GET per object, strictly worse), a store that doesn't participate (events), a metadata
// endpoint the server doesn't serve, or a delta so large that N GETs lose to one LIST.
//
// The LIST-phase concurrency slot is taken here, around whichever strategy runs, because
// both are list-heavy; Run re-enters its indefinite watch after this returns, holding
// nothing. See ListLimiter.
func (d *driver) resync(ctx context.Context) (string, error) {
	release, err := d.listLimiter.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()

	md, ok := d.store.(MetadataDiffStore)
	if !ok {
		return d.fullList(ctx)
	}
	have, err := md.SnapshotRVs(ctx)
	if err != nil {
		return "", err
	}
	// Cold cache: every object would have to be fetched anyway, and one LIST beats a
	// metadata list plus N GETs.
	if len(have) == 0 {
		return d.fullList(ctx)
	}

	// Paginated like the full LIST, and for the same reason: the metadata for a 20k-object
	// kind is small per item but not small in aggregate, and up to cacheListConcurrency
	// kinds resync at once. Each page folds straight into the diff, so only one page is
	// ever resident.
	//
	// `have` is CONSUMED as the listed set is matched: whatever remains once every page is
	// folded is exactly the set the cluster no longer has. That saves carrying a second
	// whole-collection map just to subtract one from the other.
	var changed []ObjectMeta
	var listRV string
	opts := metav1.ListOptions{Limit: listPageSize}
	for {
		metas, cont, rv, err := d.src.ListMetadata(ctx, opts)
		if err != nil {
			// Some aggregated API servers don't serve a metadata endpoint, and a continue
			// token can expire mid-pagination. Fall back rather than failing the kind —
			// the full LIST is always available, and it re-lists from the top anyway.
			slog.Debug("kubesync: metadata list failed, falling back to full list", "err", err)
			return d.fullList(ctx)
		}
		listRV = rv
		for _, m := range metas {
			if have[m.UID] != m.ResourceVersion {
				changed = append(changed, m)
			}
			delete(have, m.UID)
		}
		// A big delta: one paginated LIST beats N round trips, and there is no point
		// paging the rest of the metadata to confirm it.
		if len(changed) > d.diffThreshold {
			return d.fullList(ctx)
		}
		if cont == "" {
			break
		}
		opts.Continue = cont
	}

	// Clear the cookie before the first write, and rewrite it only once everything is
	// reconciled (below). An interrupted pass then leaves no position rather than one its
	// rows don't back — the same invariant fullList keeps by clearing on its first page.
	if len(changed) > 0 || len(have) > 0 {
		if err := md.ClearRV(ctx); err != nil {
			return "", err
		}
	}

	applied := 0
	for _, m := range changed {
		u, err := d.src.Get(ctx, m.Namespace, m.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Raced a delete between the metadata list and this GET. The object is
				// absent from the cluster, so the next pass's diff reconciles it — and it
				// was already removed from `have`, so this pass won't delete it either.
				// Harmless: one stale row for at most one resync period.
				continue
			}
			return "", err
		}
		// ApplyDiff, not ApplyChange: a GET-fetched object's resourceVersion is its own,
		// not this pass's position, so advancing the cookie to it would put the cookie
		// ahead of changes still to be applied.
		if err := md.ApplyDiff(ctx, u); err != nil {
			return "", err
		}
		applied++
	}

	// Whatever is left in `have` was not listed, so the cluster no longer has it. One
	// batched delete rather than one transaction per uid: these land on the writer
	// connection the WHOLE cache shares, so a kind that lost thousands of objects would
	// otherwise block every other kind's worker behind thousands of commits.
	vanished := make([]string, 0, len(have))
	for uid := range have {
		vanished = append(vanished, uid)
	}
	if len(vanished) > 0 {
		if err := md.DeleteByUIDs(ctx, vanished); err != nil {
			return "", err
		}
	}

	// The diff wrote through ApplyChange, which advances the cookie per object — but a
	// pass with no changed objects advances nothing, and the deletes carry no
	// resourceVersion of their own. Persist the list's RV so the next watch resumes from
	// the position this pass actually reconciled to.
	//
	// Only if the server gave us one. Persist is an unconditional upsert, and Run rejects
	// an empty or "0" RV as unusable — so writing it would trade a cookie that still
	// resumes cheaply for one the next start has to throw away and cold-LIST past, on a
	// quiet pass that changed nothing (a pass that DID write cleared the cookie above, and
	// leaving it cleared is the honest outcome).
	if usableRV(listRV) {
		if err := d.store.PersistRV(ctx, listRV); err != nil {
			return "", err
		}
	}

	// A deletion is a change but not an item pulled, so the two counts differ here.
	d.recordPass(applied+len(vanished), applied)
	return listRV, nil
}

// fullList streams a paginated full LIST into the store: each page lands as it arrives
// (bounding memory to one page of bodies), and a final Commit prunes the rows no page
// carried and persists the resume cookie. A continue token can expire (410)
// mid-pagination — the driver discards the partial pass and re-lists from the top
// (matching a client-go Reflector), bounded by maxListRestarts.
func (d *driver) fullList(ctx context.Context) (string, error) {
	// The session's first WritePage invalidates the resume cookie (rewritten only by a
	// successful Commit), so an exit at any point below — error return or 410 restart —
	// just drops the session: any cookie left is consistent with whatever pages were
	// written, and the next start cold-LISTs when partial pages exist, its prune
	// reconciling them.
	sess := d.store.BeginReplace()

	opts := metav1.ListOptions{Limit: listPageSize}
	var lastRV string
	listed, restarts := 0, 0
	for {
		items, cont, rv, err := d.src.List(ctx, opts)
		if err != nil {
			if listExpired(err) && opts.Continue != "" {
				if restarts >= maxListRestarts {
					return "", errListRestartBudget
				}
				// Discard the partial pass and re-list from the top with a fresh session.
				sess = d.store.BeginReplace()
				opts.Continue, listed = "", 0
				restarts++
				continue
			}
			return "", err
		}
		if err := sess.WritePage(ctx, items); err != nil {
			return "", err
		}
		listed += len(items)
		lastRV = rv
		if cont == "" {
			break
		}
		opts.Continue = cont
	}
	pruned, err := sess.Commit(ctx, lastRV)
	if err != nil {
		return "", err
	}
	// The prune counts as a change: mark-and-sweep is how this path DELETES, so a pass that
	// listed nothing but emptied the table did land an update — the diff path counts its
	// own vanished set for the same reason.
	d.recordPass(listed+pruned, listed)
	return lastRV, nil
}

// recordPass closes out a completed resync pass — a full LIST or a metadata diff alike.
//
// changed is what the pass actually altered in the store and items is what it announces as
// pulled; they differ only for the diff, which counts a deletion as a change but not as an
// item fetched. A completed pass is a strong PROOF: the server answered with the current
// set. It counts as a WRITE only when rows landed — an empty collection's LIST proves
// liveness but received no update, and conflating the two would let "last update received"
// tick on a cluster where nothing has happened.
//
// didResync/resyncItems are what the catch-up report reads: a pass that re-listed and left
// them unset is announced as a clean watch resume, and watchPhase then waits out the whole
// resumeGrace for a resume that never happened. They are consumed by the report — see
// fireCaughtUp.
func (d *driver) recordPass(changed, items int) {
	if d.resyncPeriod > 0 {
		d.resyncAt = d.now().Add(d.resyncDelay)
	}
	if changed > 0 {
		d.markWrite()
	} else {
		d.markProof()
	}
	d.didResync = true
	// This pass's count, not a running total. Accumulating reported the worker's whole
	// lifetime of re-pulls as one pass's work — "re-synced 2000 objects" for a pass that
	// moved 1000 — verbatim in the user-visible event log.
	d.resyncItems = items
}
