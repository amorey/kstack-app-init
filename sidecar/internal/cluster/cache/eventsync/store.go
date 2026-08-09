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

package eventsync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/store"
)

// resumeKeyPrefix names this collection's cluster_meta resume-cookie pair. It is the
// "<apiVersion>/<Kind>" shape every synced collection keys its cookie under, so the events
// pair sits in the same namespace as every per-GVR sync's.
const resumeKeyPrefix = "v1/Event"

// eventRow is one row of the cache's events table, flattened from an Event body.
type eventRow struct {
	UID          string
	InvolvedUID  string
	InvolvedKind string
	InvolvedNS   string
	InvolvedName string
	Type         string
	Reason       string
	Message      string
	FirstSeen    int64
	LastSeen     int64
	Count        int
	RawJSON      []byte
}

// eventStore is the write path into one cache's events table — the kubesync.Store the
// events worker lands its pulls in. Reads live on store.ClusterDB (Events, consumed by
// ClusterDataEventsWatch); this is the only writer, so the two stay on opposite sides of
// the same handle.
type eventStore struct {
	cdb *store.ClusterDB
	// resume is this collection's list/watch position in cluster_meta — the shared
	// protocol, since the key prefix is the only thing that differs per collection.
	resume *store.ResumeCookie
	// now supplies the updated_at stamp. A seam only so this package's tests can freeze it
	// and pin the relist sweep's boundary, which is otherwise decided by whether the
	// millisecond happened to tick between a write and the relist that follows it.
	now func() int64
}

var _ kubesync.Store = (*eventStore)(nil)

func newEventStore(cdb *store.ClusterDB) *eventStore {
	s := &eventStore{cdb: cdb, now: func() int64 { return time.Now().UnixMilli() }}
	// The cookie reads the clock through s, not a copy of it, so a test that freezes
	// s.now freezes the cookie's stamp too.
	s.resume = store.NewResumeCookie(cdb, resumeKeyPrefix, func() int64 { return s.now() })
	return s
}

// EnsureCatalog registers the Event kind in the cache's kind_catalog. The worker calls it
// on every start; the upsert makes that a no-op after the first.
//
// Events are excluded from the per-GVR object sync (one uid-keyed table, two api
// spellings — see eventsGVR), so no objectsync worker writes this row and it has to come
// from here. It matters because the catalog is what makes a kind's count reachable:
// store.Kinds is a kind_catalog LEFT JOIN over the trigger-maintained kind_counts, so
// without a catalog row the ('v1','Event') tally the events triggers keep exact is
// invisible, and the dashboard's curated Events row shows no count.
//
// There is no counterpart removal. Unlike a per-GVR sync — whose kind can stop being
// served, taking its rows and catalog entry with it — the events child exists for its
// cache's whole life, so the only thing that ends this row is the cache file being
// deleted outright.
func (s *eventStore) EnsureCatalog(ctx context.Context) error {
	return store.EnsureKindCatalog(ctx, s.cdb, store.KindRow{
		APIVersion: "v1",
		Kind:       "Event",
		Resource:   "events",
		Scope:      "Namespaced",
	})
}

// insertRow upserts one event, compressing raw_json. The single write chokepoint for
// the events table — both write paths (watch delta, relist page) route through it.
func (s *eventStore) insertRow(ctx context.Context, ex store.Execer, row eventRow, now int64) error {
	rawJSON, err := store.CompressRaw(row.RawJSON)
	if err != nil {
		return err
	}
	_, err = ex.ExecContext(ctx, `
		INSERT INTO events (
			uid, involved_uid, involved_kind, involved_ns, involved_name,
			type, reason, message, first_seen, last_seen, count, raw_json, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(uid) DO UPDATE SET
			involved_uid=excluded.involved_uid,
			involved_kind=excluded.involved_kind,
			involved_ns=excluded.involved_ns,
			involved_name=excluded.involved_name,
			type=excluded.type,
			reason=excluded.reason,
			message=excluded.message,
			first_seen=excluded.first_seen,
			last_seen=excluded.last_seen,
			count=excluded.count,
			raw_json=excluded.raw_json,
			updated_at=excluded.updated_at`,
		row.UID, store.NullIfEmpty(row.InvolvedUID), store.NullIfEmpty(row.InvolvedKind),
		store.NullIfEmpty(row.InvolvedNS), store.NullIfEmpty(row.InvolvedName),
		store.NullIfEmpty(row.Type), store.NullIfEmpty(row.Reason), store.NullIfEmpty(row.Message),
		nullableInt64(row.FirstSeen), nullableInt64(row.LastSeen), row.Count, rawJSON, now,
	)
	return err
}

// ApplyChange lands one watch delta: Added/Modified upsert, Deleted removes by uid.
func (s *eventStore) ApplyChange(ctx context.Context, t watch.EventType, u *unstructured.Unstructured) error {
	switch t {
	case watch.Added, watch.Modified:
		return s.upsert(ctx, u)
	case watch.Deleted:
		return s.remove(ctx, u)
	default:
		return nil
	}
}

func (s *eventStore) upsert(ctx context.Context, u *unstructured.Unstructured) error {
	row, err := extractEvent(u)
	if err != nil {
		return err
	}
	// The row and the cookie land together — see kubesync.Store.ApplyChange. One
	// transaction is also one acquisition of the cache's single-connection writer pool,
	// which every kind's worker shares.
	tx, err := s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path
	if err := s.insertRow(ctx, tx, row, s.now()); err != nil {
		return err
	}
	if err := s.resume.Advance(ctx, tx, u); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Wake the events watch on the dedicated events broker — not the object-write
	// broker — so an event burst never triggers the (unrelated) kind-catalog re-read.
	// The events watch coalesces + debounces these pings, so a burst still collapses
	// into one re-read.
	s.cdb.EventsNotify()
	return nil
}

func (s *eventStore) remove(ctx context.Context, u *unstructured.Unstructured) error {
	// An unkeyable delete is an ERROR, not a quiet no-op — as it is on the write path. nil
	// reported success for a delta whose cookie was never advanced, so the driver booked
	// progress and a crash in that window resumed the watch from an older position and
	// replayed. Ending the phase re-lists instead.
	uid := string(u.GetUID())
	if uid == "" {
		return fmt.Errorf("eventsync: event delete has empty UID")
	}
	tx, err := s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path
	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE uid=?`, uid); err != nil {
		return err
	}
	if err := s.resume.Advance(ctx, tx, u); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.cdb.EventsNotify()
	return nil
}

// Count returns how many events the cache currently holds — the warm-cache size the
// resume report describes itself with.
func (s *eventStore) Count(ctx context.Context) (int, error) {
	var n int
	err := s.cdb.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n)
	return n, err
}

// BeginReplace opens a streaming full-LIST reconcile of the events table. It prunes
// (in Commit): a row absent from the LIST is gone from the server, so the cache mirrors
// the server's current event set and retention is whatever the server enforces
// (--event-ttl). Every row's exit is a relist prune, an explicit watch Deleted, or both.
//
// Clearing the resume cookie is DEFERRED to the first WritePage rather than done here,
// so a pass that fails before writing anything leaves the untouched snapshot's cookie
// intact and the next start still resumes cheaply. Because a relist prunes, that clear
// is load-bearing: a pass that fails after writing pages but before Commit leaves no
// cookie, so the next start cold-LISTs and its prune reconciles the leftover rows
// instead of resuming past them.
func (s *eventStore) BeginReplace() kubesync.ReplaceSession {
	return &replaceSession{s: s, mark: s.now() + 1}
}

// replaceSession streams a paginated event relist into the events table and reconciles the
// table to what the LIST returned. The prune isn't scoped by kind — Event owns this table
// outright, so every row is in scope.
//
// It reconciles by **mark and sweep** rather than by remembering what it saw: every
// WritePage stamps `updated_at`, and Commit deletes the rows still older than the session's
// start, in one statement with no read-back. A keep-set of every uid in the collection
// would defeat the point of paginating, and a per-stale-row DELETE would hold the cache's
// single writer connection across thousands of statements.
//
// The mark is one millisecond PAST the session start, because updated_at has millisecond
// resolution and a row written in the same tick would otherwise masquerade as one this pass
// wrote.
//
// Per-page commits trade the single-transaction atomicity of the whole pass for a memory
// bound (one page of bodies at a time): a pass that fails mid-pagination leaves its
// committed pages visible until the next pass's prune reconciles them. An abandoned
// session needs no teardown — just drop it.
type replaceSession struct {
	s *eventStore
	// mark is the updated_at boundary Commit sweeps below.
	mark int64
	// cookieCleared records whether the first WritePage has durably cleared the resume
	// cookie, so the clear happens once per session.
	cookieCleared bool
}

// WritePage lands one page in its own transaction, clearing the resume cookie alongside
// the first page's rows (see BeginReplace) so partial durable state and a "sync
// completed" cookie can never coexist. An event body that won't project (no UID) is
// skipped rather than failing the page — one malformed row must not wedge the pass.
func (r *replaceSession) WritePage(ctx context.Context, items []*unstructured.Unstructured) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := r.s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path
	if !r.cookieCleared {
		if err := r.s.resume.Delete(ctx, tx); err != nil {
			return err
		}
	}
	// Stamp at or after the mark, never below it, so this page's rows survive the sweep
	// even when the whole pass runs inside one millisecond.
	now := max(r.s.now(), r.mark)
	for _, u := range items {
		row, err := extractEvent(u)
		if err != nil {
			continue
		}
		if err := r.s.insertRow(ctx, tx, row, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	r.cookieCleared = true
	// Notify per committed page, not only at Commit, so durable rows always reach the
	// events watch — a relist that commits pages then fails would otherwise leave them
	// durable-but-unannounced until some later write.
	r.s.cdb.EventsNotify()
	return nil
}

// Commit sweeps the rows no page rewrote, then persists the resume cookie. The deletes
// fire the events_kind_count triggers, keeping the kind_counts Event tally exact. Both
// cookie writes (last_list_rv + last_list_at) share the sweep's transaction, so a failed
// persist can't leave last_list_rv durably advanced — which would resume the next watch
// from a newer RV and skip the events before it.
func (r *replaceSession) Commit(ctx context.Context, resourceVersion string) (int, error) {
	tx, err := r.s.cdb.Writer().BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck // committed below; rollback is the error path

	res, err := tx.ExecContext(ctx, `DELETE FROM events WHERE updated_at < ?`, r.mark)
	if err != nil {
		return 0, err
	}
	pruned, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	if err := r.s.resume.Persist(ctx, tx, resourceVersion); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	r.s.cdb.EventsNotify()
	return int(pruned), nil
}

// PersistRV advances the resume cookie without touching event rows — called on every
// watch delta so a wake resumes from the latest position.
func (s *eventStore) PersistRV(ctx context.Context, rv string) error {
	return s.resume.Persist(ctx, s.cdb.Writer(), rv)
}

// ResumeRV returns the resume cookie to seed a watch from, or "" to force a cold
// full-LIST.
func (s *eventStore) ResumeRV(ctx context.Context) (string, error) {
	return s.resume.Get(ctx)
}

// extractEvent flattens an Event body into the events table's columns. It normalises
// both core/v1 Event and events.k8s.io/v1 Event, which carry the same data under
// different field names: the worker lists core/v1 (the api server serves every event
// there), but a body reaching us through either spelling projects the same.
func extractEvent(u *unstructured.Unstructured) (eventRow, error) {
	rawJSON, err := json.Marshal(u.Object)
	if err != nil {
		return eventRow{}, err
	}
	row := eventRow{
		UID:     string(u.GetUID()),
		RawJSON: rawJSON,
	}
	if row.UID == "" {
		return eventRow{}, fmt.Errorf("eventsync: event has empty UID")
	}

	// Branch on which field group is present, not on whether the UID is set:
	// involvedObject.uid is optional (name-only references are valid), so a missing UID
	// must not be mistaken for the events.k8s.io `regarding` spelling and clobber a real
	// involvedObject identity.
	if _, ok := u.Object["involvedObject"]; ok {
		row.InvolvedUID, _, _ = unstructured.NestedString(u.Object, "involvedObject", "uid")
		row.InvolvedKind, _, _ = unstructured.NestedString(u.Object, "involvedObject", "kind")
		row.InvolvedNS, _, _ = unstructured.NestedString(u.Object, "involvedObject", "namespace")
		row.InvolvedName, _, _ = unstructured.NestedString(u.Object, "involvedObject", "name")
	} else {
		row.InvolvedUID, _, _ = unstructured.NestedString(u.Object, "regarding", "uid")
		row.InvolvedKind, _, _ = unstructured.NestedString(u.Object, "regarding", "kind")
		row.InvolvedNS, _, _ = unstructured.NestedString(u.Object, "regarding", "namespace")
		row.InvolvedName, _, _ = unstructured.NestedString(u.Object, "regarding", "name")
	}

	row.Type, _, _ = unstructured.NestedString(u.Object, "type")
	row.Reason, _, _ = unstructured.NestedString(u.Object, "reason")
	row.Message, _, _ = unstructured.NestedString(u.Object, "message")
	if row.Message == "" {
		row.Message, _, _ = unstructured.NestedString(u.Object, "note")
	}
	// Read count from whichever spelling is present rather than treating a genuine 0 as
	// "field absent"; default to 1 only when no spelling carries it.
	count, found, _ := unstructured.NestedInt64(u.Object, "count")
	if !found {
		count, found, _ = unstructured.NestedInt64(u.Object, "series", "count")
	}
	if !found {
		count, found, _ = unstructured.NestedInt64(u.Object, "deprecatedCount")
	}
	if !found {
		count = 1
	}
	row.Count = int(count)

	row.FirstSeen = firstParsedTime(u, []string{"firstTimestamp"})
	row.LastSeen = firstParsedTime(u,
		[]string{"lastTimestamp"},
		[]string{"series", "lastObservedTime"},
		[]string{"deprecatedLastTimestamp"},
		[]string{"eventTime"},
	)
	if row.FirstSeen == 0 {
		row.FirstSeen = row.LastSeen
	}
	return row, nil
}

// firstParsedTime reads the given fields in order and returns the first that both is
// present AND parses, as epoch millis; 0 when none does.
//
// Both halves matter. The api server spells an Event's time four ways across its two
// versions, so the chain has to try each; and it must advance on a value it cannot READ,
// not merely on an absent one. Branching on presence alone meant a malformed
// series.lastObservedTime ended the chain: last_seen stored NULL, and since the events
// read orders by last_seen DESC, the newest event in the cluster sorted below every
// timestamped row and never appeared in the dashboard's window.
//
// RFC3339Nano is the one layout — it accepts a plain RFC3339 stamp too, so the
// second-precision spellings need no separate pass.
func firstParsedTime(u *unstructured.Unstructured, paths ...[]string) int64 {
	for _, path := range paths {
		s, _, _ := unstructured.NestedString(u.Object, path...)
		if s == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixMilli()
		}
	}
	return 0
}

// nullableInt64 writes NULL for a zero timestamp so a missing time reads as absence, not
// a bogus 1970 instant: an event with no lastTimestamp/eventTime stores NULL, which the
// read side surfaces as a null wire timestamp.
func nullableInt64(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}
