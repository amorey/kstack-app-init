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

// Every statement the store issues, named once and compiled once per connection.
// modernc has no compiled-statement cache — it prepares, runs and finalizes on every
// call — so a relist page that runs six statements per object would otherwise pay a
// full sqlite3_prepare_v2 per row.
package kubestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// stmtID names one statement. The text lives in stmtText and never at a call site, so
// there is one text, one id and one compilation.
type stmtID int

const (
	stmtUpsertObject stmtID = iota
	stmtInsertStatusTransition
	stmtDeleteObject
	stmtSweepObjects
	// The per-object cascade: one point delete per side table.
	stmtDeleteLabelsOfObject
	stmtDeleteOwnerRefsOfChild
	stmtDeleteOwnerRefsOwnedBy
	stmtDeleteStatusHistoryOfObject
	// The same four over the uid list a sweep returned.
	stmtDeleteLabelsOfObjects
	stmtDeleteOwnerRefsOfChildren
	stmtDeleteOwnerRefsOwnedByAny
	stmtDeleteStatusHistoryOfObjects
	stmtUpsertOwnerRefs
	stmtUpsertLabels
	// One kind's rows, for a kind that has stopped being synced.
	stmtClearOwnerRefsOfKind
	stmtClearLabelsOfKind
	stmtClearStatusHistoryOfKind
	stmtClearObjectsOfKind
	stmtDeleteKindCount
	stmtUpsertEvent
	stmtDeleteEvent
	stmtDeleteAllEvents
	stmtPruneEvents
	stmtUpsertMeta
	stmtDeleteMeta
	stmtResolveKindRename
	stmtUpsertKind
	stmtDeleteAllKinds
	stmtPruneKinds
	stmtSweepStatusHistory
	stmtSelectMeta
	stmtCountKind
	stmtSelectKinds
	stmtSelectEvents
	stmtSelectObjectBody
	stmtSelectObjects
	numStmts int = iota
)

// stmtText is the SQL each id stands for.
var stmtText = [numStmts]string{
	stmtUpsertObject: `
		INSERT INTO objects (
			uid, api_version, kind, namespace, name,
			resource_version, generation, created_at, updated_at, raw_json,
			status_summary, ready_count, total_count, restart_count, host
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(uid) DO UPDATE SET
			api_version=excluded.api_version,
			kind=excluded.kind,
			namespace=excluded.namespace,
			name=excluded.name,
			resource_version=excluded.resource_version,
			generation=excluded.generation,
			-- creationTimestamp is immutable, so a body without it carries no news;
			-- projectObject leaves it 0, which would otherwise overwrite a good value
			-- with the epoch.
			created_at=CASE WHEN excluded.created_at > 0 THEN excluded.created_at ELSE created_at END,
			updated_at=excluded.updated_at,
			raw_json=excluded.raw_json,
			status_summary=excluded.status_summary,
			ready_count=excluded.ready_count,
			total_count=excluded.total_count,
			restart_count=excluded.restart_count,
			host=excluded.host`,

	stmtInsertStatusTransition: `
		INSERT INTO status_history(uid, at, summary)
		SELECT ?, ?, ?
		WHERE NOT EXISTS (SELECT 1 FROM objects WHERE uid = ? AND status_summary = ?)`,

	stmtDeleteObject: `DELETE FROM objects WHERE uid=?`,
	stmtSweepObjects: `DELETE FROM objects WHERE api_version=? AND kind=? AND updated_at < ? RETURNING uid`,

	stmtDeleteLabelsOfObject:        `DELETE FROM labels WHERE uid=?`,
	stmtDeleteOwnerRefsOfChild:      `DELETE FROM owner_refs WHERE child_uid=?`,
	stmtDeleteOwnerRefsOwnedBy:      `DELETE FROM owner_refs WHERE owner_uid=?`,
	stmtDeleteStatusHistoryOfObject: `DELETE FROM status_history WHERE uid=?`,

	stmtDeleteLabelsOfObjects:        `DELETE FROM labels WHERE uid IN (SELECT value FROM json_each(?))`,
	stmtDeleteOwnerRefsOfChildren:    `DELETE FROM owner_refs WHERE child_uid IN (SELECT value FROM json_each(?))`,
	stmtDeleteOwnerRefsOwnedByAny:    `DELETE FROM owner_refs WHERE owner_uid IN (SELECT value FROM json_each(?))`,
	stmtDeleteStatusHistoryOfObjects: `DELETE FROM status_history WHERE uid IN (SELECT value FROM json_each(?))`,

	// WHERE true is required on both: without it SQLite parses ON CONFLICT as a join
	// constraint on the SELECT and the statement is a syntax error at DO.
	stmtUpsertOwnerRefs: `
		INSERT INTO owner_refs (child_uid, owner_uid, is_controller)
		SELECT ?1, value ->> 0, value ->> 1 FROM json_each(?2) WHERE true
		ON CONFLICT(child_uid, owner_uid) DO UPDATE SET is_controller=excluded.is_controller`,
	stmtUpsertLabels: `
		INSERT INTO labels (uid, key, value)
		SELECT ?1, key, value FROM json_each(?2) WHERE true
		ON CONFLICT(uid, key) DO UPDATE SET value=excluded.value`,

	stmtClearOwnerRefsOfKind:     `DELETE FROM owner_refs WHERE child_uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
	stmtClearLabelsOfKind:        `DELETE FROM labels WHERE uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
	stmtClearStatusHistoryOfKind: `DELETE FROM status_history WHERE uid IN (SELECT uid FROM objects WHERE api_version = ? AND kind = ?)`,
	stmtClearObjectsOfKind:       `DELETE FROM objects WHERE api_version = ? AND kind = ?`,
	stmtDeleteKindCount:          `DELETE FROM kind_counts WHERE api_version = ? AND kind = ?`,

	stmtUpsertEvent: `
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
	stmtDeleteEvent:     `DELETE FROM events WHERE uid=?`,
	stmtDeleteAllEvents: `DELETE FROM events`,
	stmtPruneEvents:     `DELETE FROM events WHERE updated_at < ?`,

	stmtUpsertMeta: `
		INSERT INTO cluster_meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
	stmtDeleteMeta: `DELETE FROM cluster_meta WHERE key = ?`,

	stmtResolveKindRename: `DELETE FROM kind_catalog WHERE api_version = ? AND resource = ? AND kind <> ?`,
	// schema_json is deliberately absent from the update: nothing here fills it, and
	// writing NULL on every sweep would make the column unusable to whoever does.
	// printer_columns is the opposite case — the sweep is what fills it, so it rides the
	// update, or a CRD that drops a column would keep being served with it.
	stmtUpsertKind: `
		INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd, printer_columns)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(api_version, kind) DO UPDATE SET
			resource = excluded.resource, scope = excluded.scope, is_crd = excluded.is_crd,
			printer_columns = excluded.printer_columns`,
	stmtDeleteAllKinds: `DELETE FROM kind_catalog`,
	stmtPruneKinds: `
		DELETE FROM kind_catalog
		WHERE (api_version, kind) NOT IN (SELECT value ->> 0, value ->> 1 FROM json_each(?))`,

	stmtSweepStatusHistory: `DELETE FROM status_history WHERE at < ?`,

	stmtSelectMeta: `SELECT value FROM cluster_meta WHERE key = ?`,
	stmtCountKind:  `SELECT count FROM kind_counts WHERE api_version=? AND kind=?`,
	stmtSelectKinds: `
		SELECT kc.api_version, kc.kind, kc.resource, kc.scope, kc.is_crd,
		       COALESCE(kc.printer_columns, ''), COALESCE(knt.count, 0)
		FROM kind_catalog kc
		LEFT JOIN kind_counts knt ON knt.api_version = kc.api_version AND knt.kind = kc.kind
		ORDER BY kc.api_version, kc.kind`,
	stmtSelectEvents: `
		SELECT uid,
		       COALESCE(type, ''), COALESCE(reason, ''), COALESCE(message, ''),
		       COALESCE(count, 0), COALESCE(first_seen, 0), COALESCE(last_seen, 0),
		       COALESCE(involved_kind, ''), COALESCE(involved_ns, ''), COALESCE(involved_name, '')
		FROM events
		ORDER BY last_seen DESC, uid DESC`,
	stmtSelectObjectBody: `SELECT raw_json FROM objects WHERE uid = ?`,
	stmtSelectObjects: `
		SELECT uid, api_version, kind, namespace, name, resource_version, created_at
		FROM objects
		WHERE api_version = ?
		  AND kind = (SELECT kind FROM kind_catalog WHERE api_version = ? AND resource = ?)
		ORDER BY namespace, name`,
}

// stmtWrites says which pool an id runs on. Hand-maintained beside the text, and it
// drives both halves of its own enforcement — the reader prepares by it and a call
// routes by it — so nothing at runtime can catch a wrong entry. The cross-check in
// statements_test.go is what does.
var stmtWrites = [numStmts]bool{
	stmtUpsertObject:                 true,
	stmtInsertStatusTransition:       true,
	stmtDeleteObject:                 true,
	stmtSweepObjects:                 true,
	stmtDeleteLabelsOfObject:         true,
	stmtDeleteOwnerRefsOfChild:       true,
	stmtDeleteOwnerRefsOwnedBy:       true,
	stmtDeleteStatusHistoryOfObject:  true,
	stmtDeleteLabelsOfObjects:        true,
	stmtDeleteOwnerRefsOfChildren:    true,
	stmtDeleteOwnerRefsOwnedByAny:    true,
	stmtDeleteStatusHistoryOfObjects: true,
	stmtUpsertOwnerRefs:              true,
	stmtUpsertLabels:                 true,
	stmtClearOwnerRefsOfKind:         true,
	stmtClearLabelsOfKind:            true,
	stmtClearStatusHistoryOfKind:     true,
	stmtClearObjectsOfKind:           true,
	stmtDeleteKindCount:              true,
	stmtUpsertEvent:                  true,
	stmtDeleteEvent:                  true,
	stmtDeleteAllEvents:              true,
	stmtPruneEvents:                  true,
	stmtUpsertMeta:                   true,
	stmtDeleteMeta:                   true,
	stmtResolveKindRename:            true,
	stmtUpsertKind:                   true,
	stmtDeleteAllKinds:               true,
	stmtPruneKinds:                   true,
	stmtSweepStatusHistory:           true,
}

// prepareStatements compiles the half of the set one pool serves — the writer takes the
// writes, the reader the reads — so a statement that will not compile fails the cache's
// open rather than the call that first reaches it. Splitting them is what stmt() can
// express: it picks the pool from stmtWrites alone, so a read compiled on the writer
// would be unreachable.
//
// Each compiles on the connection this call lands on and is cached there; the pool's
// other connections compile it the first time they serve it.
func prepareStatements(ctx context.Context, db *sql.DB, writes bool) ([numStmts]*sql.Stmt, error) {
	var out [numStmts]*sql.Stmt
	for id := range stmtID(numStmts) {
		if stmtWrites[id] != writes {
			continue
		}
		st, err := db.PrepareContext(ctx, stmtText[id])
		if err != nil {
			closeStatements(out)
			return out, fmt.Errorf("prepare %q: %w", stmtText[id], err)
		}
		out[id] = st
	}
	return out, nil
}

// closeStatements finalizes a prepared set. A nil slot is an id this pool does not serve.
func closeStatements(set [numStmts]*sql.Stmt) error {
	var errs []error
	for _, st := range set {
		if st != nil {
			errs = append(errs, st.Close())
		}
	}
	return errors.Join(errs...)
}

// stmts issues the file's prepared statements, on tx when there is one and on the pool
// when there is not — a delta writes straight on the writer, a relist page inside its
// transaction, and one helper serves both.
type stmts struct {
	f  *file
	tx *sql.Tx // nil: run on the pool
	// bound holds the transaction's rebinding of each id. Tx.StmtContext allocates a
	// fresh statement per call and registers it db-wide, released only at commit, so a
	// relist page would otherwise pile up one per statement per object. The map is shared
	// by every copy of this value, and needs no lock for the reason the transaction it
	// belongs to needs none: one goroutine owns it.
	bound map[stmtID]*sql.Stmt
}

// stmts issues on the pool; tx issues inside one transaction.
func (f *file) stmts() stmts { return stmts{f: f} }
func (f *file) tx(tx *sql.Tx) stmts {
	return stmts{f: f, tx: tx, bound: make(map[stmtID]*sql.Stmt)}
}

// stmt resolves an id to a prepared statement, rebound onto the transaction when there is
// one. The pool is stmtWrites' call, never the helper's: a write that returns rows still
// runs on the writer.
//
// Tx.StmtContext refuses a statement prepared on another pool outright, so a read that
// has to run inside a write transaction needs its id prepared on the writer as well —
// which a single bool cannot say. No such read exists; adding one is a design decision,
// not a flag flip.
func (s stmts) stmt(ctx context.Context, id stmtID) *sql.Stmt {
	prepared := s.f.readStmts[id]
	if stmtWrites[id] {
		prepared = s.f.writeStmts[id]
	}
	if s.tx == nil {
		return prepared
	}
	if bound, ok := s.bound[id]; ok {
		return bound
	}
	bound := s.tx.StmtContext(ctx, prepared)
	s.bound[id] = bound
	return bound
}

func (s stmts) exec(ctx context.Context, id stmtID, args ...any) (sql.Result, error) {
	return s.stmt(ctx, id).ExecContext(ctx, args...)
}

func (s stmts) query(ctx context.Context, id stmtID, args ...any) (*sql.Rows, error) {
	return s.stmt(ctx, id).QueryContext(ctx, args...)
}

func (s stmts) queryRow(ctx context.Context, id stmtID, args ...any) *sql.Row {
	return s.stmt(ctx, id).QueryRowContext(ctx, args...)
}
