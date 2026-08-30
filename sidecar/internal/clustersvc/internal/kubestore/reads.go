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

// The read side: what a watch re-reads on every ping, and what a list serves once.
// Every query here rides the reader pool, never the writer.
package kubestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

// readerPoolSize caps concurrent read connections per cache. Each open connection costs
// memory and a file descriptor, and the readers are the watches — a handful per window.
const readerPoolSize = 4

// Kinds reads the discovered kind catalog, ordered for stable display.
//
// The join is OUTER because the two tables answer different questions: kind_catalog is what
// the cluster serves, kind_counts is what has been cached. An advertised kind with nothing
// synced yet reads Count 0 rather than dropping out of the nav. Count comes off the
// trigger-maintained counts, so this is O(kinds) and never a scan of objects.
func (s *Store) Kinds(ctx context.Context) ([]KindRow, error) {
	rows, _, _, err := s.KindsWithFingerprint(ctx)
	return rows, err
}

// KindsWithFingerprint is Kinds beside the fingerprint of the sweep that wrote the rows,
// and ok reports whether a sweep recorded one at all. **Both come out of one read
// transaction**: a caller compares the fingerprint to decide whether the rows are the
// sweep's answer, and a pair read from two snapshots can pass that check while carrying a
// wipe's empty table — which reads as a cluster that serves nothing.
//
// An unrecorded fingerprint is a table no sweep has written, and so is one that will not
// parse. Neither is an error: it is what a fresh file, and a wiped one, look like.
func (s *Store) KindsWithFingerprint(ctx context.Context) ([]KindRow, uint64, bool, error) {
	f, err := s.file()
	if err != nil {
		return nil, 0, false, err
	}
	tx, err := f.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, false, fmt.Errorf("read kinds: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // a read transaction, never committed
	st := f.tx(tx)

	rows, err := st.query(ctx, stmtSelectKinds)
	if err != nil {
		return nil, 0, false, fmt.Errorf("read kinds: %w", err)
	}
	defer rows.Close()

	var out []KindRow
	for rows.Next() {
		var r KindRow
		if err := rows.Scan(&r.APIVersion, &r.Kind, &r.Resource, &r.Scope, &r.IsCRD,
			&r.PrinterColumns, &r.Count); err != nil {
			return nil, 0, false, fmt.Errorf("read kinds: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("read kinds: %w", err)
	}

	v, ok, err := getMeta(ctx, st, kindsFingerprintKey)
	if err != nil {
		return nil, 0, false, fmt.Errorf("read kinds fingerprint: %w", err)
	}
	if !ok {
		return out, 0, false, nil
	}
	fingerprint, parseErr := strconv.ParseUint(v, 10, 64)
	return out, fingerprint, parseErr == nil, nil
}

// EventRow is one cached Kubernetes Event, flattened for display. The compressed body is
// deliberately not read: nothing serves it.
type EventRow struct {
	// UID is the Event's own object UID — the watch key.
	UID     string
	Type    string
	Reason  string
	Message string
	// Count is the coalesced series count, >= 1.
	Count int
	// FirstSeen and LastSeen are unix-millis, 0 when the source carried none.
	FirstSeen int64
	LastSeen  int64
	// The object the event is about; any of the three may be empty.
	InvolvedKind string
	InvolvedNS   string
	InvolvedName string
}

// EventsWithHead reads every cached event, newest first, beside the position it is current
// at. The uid tiebreak makes that order total: last_seen has one-second resolution, and a
// relist re-inserts every row with fresh rowids, so rows tied on a second would otherwise
// reshuffle between two reads.
//
// **The rows and the head come out of one transaction**, as in KindsWithFingerprint: the
// caller resumes from that position, so a write landing between two reads would leave the
// cursor claiming rows nobody was sent.
func (s *Store) EventsWithHead(ctx context.Context) ([]EventRow, int64, error) {
	f, err := s.file()
	if err != nil {
		return nil, 0, err
	}
	var (
		out []EventRow
		at  int64
	)
	err = inReadTx(ctx, f, "read events", func(st stmts) error {
		var err error
		if out, err = scanEvents(ctx, st, stmtSelectEvents); err != nil {
			return err
		}
		at, err = head(ctx, st)
		return err
	})
	return out, at, err
}

// inReadTx runs fn inside one read-only transaction on the reader pool, wrapping whatever
// it returns with what. Every read that pairs rows with a position takes one: the two must
// come out of the same snapshot, or the position covers rows the caller was never handed.
func inReadTx(ctx context.Context, f *file, what string, fn func(st stmts) error) error {
	tx, err := f.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("%s: begin: %w", what, err)
	}
	defer tx.Rollback() //nolint:errcheck // a read transaction, never committed
	if err := fn(f.tx(tx)); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// scanEvents runs one of the events reads and collects its rows.
func scanEvents(ctx context.Context, st stmts, id stmtID, args ...any) ([]EventRow, error) {
	rows, err := st.query(ctx, id, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		if err := rows.Scan(
			&r.UID, &r.Type, &r.Reason, &r.Message, &r.Count,
			&r.FirstSeen, &r.LastSeen, &r.InvolvedKind, &r.InvolvedNS, &r.InvolvedName,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ObjectRow is one cached object of any kind: the identity a table renders and sorts on.
// The body is deliberately absent — a watch re-reads the whole collection on every ping and
// only the rows that become frames need one, which ObjectBody fetches by uid.
type ObjectRow struct {
	UID        string
	APIVersion string
	Kind       string
	// Namespace is empty for a cluster-scoped kind.
	Namespace string
	Name      string
	// ResourceVersion is what the objects diff keys on beside the UID: the server bumps it
	// on every write, so it says an in-place edit happened without inflating the body.
	ResourceVersion string
	// CreatedAt is creationTimestamp as unix-millis, 0 if absent.
	CreatedAt int64
}

// ObjectBody is one object's stored body, decompressed. Separate from the row because the
// watch diffs on (uid, resourceVersion) and only the rows that become frames need one.
//
// The bool is false when the row is gone — deleted between the diff read that named it and
// this call, where the next resync's Deleted frame is the answer and a read failure would
// be reporting a race as a breakage.
func (s *Store) ObjectBody(ctx context.Context, uid string) ([]byte, bool, error) {
	f, err := s.file()
	if err != nil {
		return nil, false, err
	}
	var stored []byte
	err = f.stmts().queryRow(ctx, stmtSelectObjectBody, uid).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read object body %s: %w", uid, err)
	}
	body, err := decompressRaw(stored)
	if err != nil {
		return nil, false, fmt.Errorf("read object body %s: %w", uid, err)
	}
	return body, true, nil
}

// ObjectsWithHead reads one kind's whole cached set, ordered by (namespace, name), beside
// the position it is current at — one transaction, for the reason EventsWithHead states.
//
// The caller names the plural while the table is keyed by Kind, so the resource translates
// through kind_catalog — whose one writer is the discovery sweep. A kind with no catalog row
// therefore reads empty, however many rows its worker has synced.
//
// **No body.** A caller that wants whole rows gets its own query rather than putting
// raw_json back on this one — the watch's diff would then hold and re-read every body in the
// collection to learn that three rows moved.
func (s *Store) ObjectsWithHead(ctx context.Context, apiVersion, resource string) ([]ObjectRow, int64, error) {
	f, err := s.file()
	if err != nil {
		return nil, 0, err
	}
	var (
		out []ObjectRow
		at  int64
	)
	err = inReadTx(ctx, f, "read objects", func(st stmts) error {
		var err error
		if out, err = scanObjects(ctx, st, stmtSelectObjects, apiVersion, apiVersion, resource); err != nil {
			return err
		}
		at, err = head(ctx, st)
		return err
	})
	return out, at, err
}

// scanObjects runs one of the object reads and collects its rows.
func scanObjects(ctx context.Context, st stmts, id stmtID, args ...any) ([]ObjectRow, error) {
	rows, err := st.query(ctx, id, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ObjectRow
	for rows.Next() {
		var r ObjectRow
		if err := rows.Scan(&r.UID, &r.APIVersion, &r.Kind, &r.Namespace, &r.Name,
			&r.ResourceVersion, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
