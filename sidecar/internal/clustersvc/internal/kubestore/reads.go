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

	rows, err := tx.QueryContext(ctx,
		`SELECT kc.api_version, kc.kind, kc.resource, kc.scope, kc.is_crd, COALESCE(knt.count, 0)
		 FROM kind_catalog kc
		 LEFT JOIN kind_counts knt ON knt.api_version = kc.api_version AND knt.kind = kc.kind
		 ORDER BY kc.api_version, kc.kind`)
	if err != nil {
		return nil, 0, false, fmt.Errorf("read kinds: %w", err)
	}
	defer rows.Close()

	var out []KindRow
	for rows.Next() {
		var r KindRow
		if err := rows.Scan(&r.APIVersion, &r.Kind, &r.Resource, &r.Scope, &r.IsCRD, &r.Count); err != nil {
			return nil, 0, false, fmt.Errorf("read kinds: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("read kinds: %w", err)
	}

	v, ok, err := getMeta(ctx, tx, kindsFingerprintKey)
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

// Events reads every cached event, newest first. The uid tiebreak makes that order total:
// last_seen has one-second resolution, and a relist re-inserts every row with fresh rowids,
// so rows tied on a second would otherwise reshuffle between two reads.
func (s *Store) Events(ctx context.Context) ([]EventRow, error) {
	f, err := s.file()
	if err != nil {
		return nil, err
	}
	rows, err := f.readDB.QueryContext(ctx,
		`SELECT uid,
		        COALESCE(type, ''), COALESCE(reason, ''), COALESCE(message, ''),
		        COALESCE(count, 0), COALESCE(first_seen, 0), COALESCE(last_seen, 0),
		        COALESCE(involved_kind, ''), COALESCE(involved_ns, ''), COALESCE(involved_name, '')
		 FROM events
		 ORDER BY last_seen DESC, uid DESC`)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		if err := rows.Scan(
			&r.UID, &r.Type, &r.Reason, &r.Message, &r.Count,
			&r.FirstSeen, &r.LastSeen, &r.InvolvedKind, &r.InvolvedNS, &r.InvolvedName,
		); err != nil {
			return nil, fmt.Errorf("read events: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	return out, nil
}

// ObjectRow is one cached object of any kind: the identity a table renders and sorts on,
// plus the stored body.
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
	// CompressedJSON is the body AS STORED — zlib, not JSON. Named for what it holds
	// because a caller that forwarded it unchanged would serve compressed bytes as the
	// JSON scalar; Body is what turns it into one.
	CompressedJSON []byte
}

// Body is the object's JSON, decompressed. Deliberately not done by the read: a watch
// re-reads the whole collection on every ping and only the rows that become frames need a
// body, so inflating them all would undo what keying the diff on the resourceVersion buys.
func (r ObjectRow) Body() ([]byte, error) { return decompressRaw(r.CompressedJSON) }

// Objects reads one kind's whole cached set, ordered by (namespace, name).
//
// The caller names the plural while the table is keyed by Kind, so the resource translates
// through kind_catalog — whose one writer is the discovery sweep. A kind with no catalog row
// therefore reads empty, however many rows its worker has synced.
//
// The body comes back compressed: the watch diffs on (uid, resource_version) and only the
// rows that become frames are ever inflated.
func (s *Store) Objects(ctx context.Context, apiVersion, resource string) ([]ObjectRow, error) {
	f, err := s.file()
	if err != nil {
		return nil, err
	}
	rows, err := f.readDB.QueryContext(ctx,
		`SELECT uid, api_version, kind, namespace, name, resource_version, created_at, raw_json
		 FROM objects
		 WHERE api_version = ?
		   AND kind = (SELECT kind FROM kind_catalog WHERE api_version = ? AND resource = ?)
		 ORDER BY namespace, name`,
		apiVersion, apiVersion, resource)
	if err != nil {
		return nil, fmt.Errorf("read objects: %w", err)
	}
	defer rows.Close()

	var out []ObjectRow
	for rows.Next() {
		var r ObjectRow
		if err := rows.Scan(&r.UID, &r.APIVersion, &r.Kind, &r.Namespace, &r.Name,
			&r.ResourceVersion, &r.CreatedAt, &r.CompressedJSON); err != nil {
			return nil, fmt.Errorf("read objects: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read objects: %w", err)
	}
	return out, nil
}
