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

// resumeKeyPrefix names this collection's cluster_meta cookie pair, in the
// "<apiVersion>/<Kind>" namespace every synced collection shares.
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

// eventStore is the only writer into one cache's events table; reads live on
// store.ClusterDB.Events.
type eventStore struct {
	cdb    *store.ClusterDB
	resume *store.ResumeCookie
	// now supplies the updated_at stamp; a seam so tests can freeze it and pin the relist
	// sweep's boundary.
	now func() int64
}

var _ kubesync.Store = (*eventStore)(nil)

func newEventStore(cdb *store.ClusterDB) *eventStore {
	s := &eventStore{cdb: cdb, now: func() int64 { return time.Now().UnixMilli() }}
	// Read the clock through s, not a copy, so freezing s.now freezes the cookie too.
	s.resume = store.NewResumeCookie(cdb, resumeKeyPrefix, func() int64 { return s.now() })
	return s
}

// EnsureCatalog registers the Event kind in kind_catalog. Events are excluded from the
// per-GVR object sync, so no objectsync worker writes this row — and without it the
// ('v1','Event') tally kept by the events triggers is unreachable (store.Kinds joins
// counts onto the catalog), leaving the dashboard's Events row countless.
//
// No counterpart removal: the events child lives as long as its cache, so only deleting
// the cache file ends this row.
func (s *eventStore) EnsureCatalog(ctx context.Context) error {
	return store.EnsureKindCatalog(ctx, s.cdb, store.KindRow{
		APIVersion: "v1",
		Kind:       "Event",
		Resource:   "events",
		Scope:      "Namespaced",
	})
}

// insertRow upserts one event, compressing raw_json — the single write chokepoint both
// the watch-delta and relist-page paths route through.
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
	// Row + cookie together (see kubesync.Store.ApplyChange); one transaction is also one
	// acquisition of the cache's single-connection writer pool.
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
	// The dedicated events broker, so a burst never drives the object watches.
	s.cdb.EventsNotify()
	return nil
}

func (s *eventStore) remove(ctx context.Context, u *unstructured.Unstructured) error {
	// An unkeyable delete errors rather than no-opping: nil would book progress for a
	// delta whose cookie never advanced, so a crash there would resume older and replay.
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

// Count returns the cached event count — the warm-cache size a resume reports.
func (s *eventStore) Count(ctx context.Context) (int, error) {
	var n int
	err := s.cdb.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&n)
	return n, err
}

// BeginReplace opens a streaming full-LIST reconcile. Commit prunes: a row absent from
// the LIST is gone from the server, so retention is whatever the server enforces
// (--event-ttl).
//
// Clearing the resume cookie is DEFERRED to the first WritePage, so a pass failing before
// any write keeps the intact snapshot's cookie, while one failing after writing leaves no
// cookie and the next start cold-LISTs and prunes the leftovers.
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
func (s *eventStore) BeginReplace() kubesync.ReplaceSession {
	return &replaceSession{s: s, mark: s.now() + 1}
}

// replaceSession streams a paginated relist into the events table (Event owns the table
// outright, so the prune is unscoped).
//
// It reconciles by MARK AND SWEEP, not a keep-set: every WritePage stamps updated_at and
// Commit deletes what's still older, in one statement. A keep-set of every uid would
// defeat pagination, and per-row DELETEs would hold the single writer for thousands of
// statements. The mark is 1ms PAST the session start, since updated_at has millisecond
// resolution and a same-tick row would otherwise look like one this pass wrote.
//
// Per-page commits trade whole-pass atomicity for a one-page memory bound: a pass failing
// mid-pagination leaves committed pages visible until the next pass prunes them.
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
type replaceSession struct {
	s *eventStore
	// mark is the updated_at boundary Commit sweeps below.
	mark int64
	// cookieCleared makes the first WritePage's cookie clear happen once per session.
	cookieCleared bool
}

// WritePage lands one page in its own transaction, clearing the resume cookie alongside
// the first page's rows (see BeginReplace) so partial rows and a "sync completed" cookie
// can never coexist. A body that won't project (no UID) is skipped, not fatal.
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
	// Never below the mark, so this page survives the sweep even if the whole pass runs
	// inside one millisecond.
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
	// Per committed page, not only at Commit: a relist that commits pages then fails
	// would otherwise leave durable rows unannounced until some later write.
	r.s.cdb.EventsNotify()
	return nil
}

// Commit sweeps the rows no page rewrote, then persists the cookie in the same
// transaction — a failed persist must not leave last_list_rv durably advanced, which
// would resume the next watch past the events before it. The deletes fire the
// events_kind_count triggers, keeping the Event tally exact.
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

// PersistRV advances the resume cookie without touching rows — the bookmark path.
func (s *eventStore) PersistRV(ctx context.Context, rv string) error {
	return s.resume.Persist(ctx, s.cdb.Writer(), rv)
}

// ResumeRV returns the cookie to seed a watch from, or "" to force a cold full LIST.
func (s *eventStore) ResumeRV(ctx context.Context) (string, error) {
	return s.resume.Get(ctx)
}

// extractEvent flattens an Event body into the events columns, normalising both core/v1
// and events.k8s.io/v1 spellings of the same data into one row.
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

	// Branch on which field group is present, not on the UID: involvedObject.uid is
	// optional, so a missing one must not be read as the `regarding` spelling.
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
	// Take the first spelling PRESENT, so a genuine 0 isn't read as absent; default to 1
	// only when none carries it.
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

// firstParsedTime returns the first of the given fields that is present AND parses, as
// epoch millis (0 if none). Advancing on unparseable — not merely absent — values is
// load-bearing: a malformed series.lastObservedTime would otherwise store NULL last_seen
// and sort the cluster's newest event below every timestamped row. RFC3339Nano also
// accepts plain RFC3339, so second-precision spellings need no extra pass.
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

// nullableInt64 writes NULL for a zero timestamp, so a missing time reads as absence
// rather than a bogus 1970 instant.
func nullableInt64(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}
