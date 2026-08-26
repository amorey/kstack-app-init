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

package kubestore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The events table's write path. Events are their own table because they carry columns
// no object has (first_seen, last_seen, count) and are read by a different query — the
// newest window, not a kind's contents.

// eventRow is one row of the events table, flattened from an Event body.
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

// extractEvent flattens an Event body into the events columns, normalising both the
// core/v1 and events.k8s.io/v1 spellings of the same data into one row shape.
func extractEvent(u *unstructured.Unstructured) (eventRow, error) {
	if u == nil || u.Object == nil {
		return eventRow{}, fmt.Errorf("kubestore: event is empty")
	}
	// Through the same sanitize as an object: an event carries managedFields like
	// anything else, and this is the highest-volume table in the file.
	rawJSON, err := json.Marshal(sanitize(u).Object)
	if err != nil {
		return eventRow{}, err
	}
	row := eventRow{UID: string(u.GetUID()), RawJSON: rawJSON}
	if row.UID == "" {
		return eventRow{}, fmt.Errorf("kubestore: event has empty UID")
	}

	// Branch on which field group is present, not on the UID: involvedObject.uid is
	// optional, so a missing one must not be read as the `regarding` spelling.
	group := "regarding"
	if _, ok := u.Object["involvedObject"]; ok {
		group = "involvedObject"
	}
	row.InvolvedUID, _, _ = unstructured.NestedString(u.Object, group, "uid")
	row.InvolvedKind, _, _ = unstructured.NestedString(u.Object, group, "kind")
	row.InvolvedNS, _, _ = unstructured.NestedString(u.Object, group, "namespace")
	row.InvolvedName, _, _ = unstructured.NestedString(u.Object, group, "name")

	row.Type, _, _ = unstructured.NestedString(u.Object, "type")
	row.Reason, _, _ = unstructured.NestedString(u.Object, "reason")
	row.Message, _, _ = unstructured.NestedString(u.Object, "message")
	if row.Message == "" {
		row.Message, _, _ = unstructured.NestedString(u.Object, "note")
	}

	// Take the first spelling PRESENT, so a genuine 0 is not read as absent; default to
	// 1 only when none carries it.
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
// epoch millis (0 if none). Advancing past an unparseable — not merely absent — value is
// load-bearing: a malformed series.lastObservedTime would otherwise store a NULL
// last_seen and sort the cluster's newest event below every timestamped row.
// RFC3339Nano also accepts plain RFC3339, so second-precision spellings need no extra
// pass.
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

// insertEventRow upserts one event by UID — the chokepoint both the watch-delta and
// relist-page paths route through.
func insertEventRow(ctx context.Context, ex execer, row eventRow, now int64) error {
	rawJSON, err := compressRaw(row.RawJSON)
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
		row.UID, nullIfEmpty(row.InvolvedUID), nullIfEmpty(row.InvolvedKind),
		nullIfEmpty(row.InvolvedNS), nullIfEmpty(row.InvolvedName),
		nullIfEmpty(row.Type), nullIfEmpty(row.Reason), nullIfEmpty(row.Message),
		nullableInt64(row.FirstSeen), nullableInt64(row.LastSeen), row.Count, rawJSON, now,
	)
	return err
}

// nullableInt64 writes NULL for a zero timestamp, so a missing time reads as absence
// rather than a bogus 1970 instant.
func nullableInt64(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}
