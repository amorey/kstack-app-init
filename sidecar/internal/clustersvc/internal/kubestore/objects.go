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
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The objects table's write path: how one body becomes a row, and the statements that
// land it. Every statement is kind-scoped — the table is shared by every kind a cache
// mirrors, and concurrent workers touch disjoint rows.

// redactedValue replaces every Secret data value at write time; keys are kept so a UI
// can list what a Secret holds.
const redactedValue = "[redacted]"

// objectRow is one row of the objects table, flattened from an object body.
type objectRow struct {
	UID             string
	Namespace       string
	Name            string
	ResourceVersion string
	Generation      int64
	CreatedAt       int64
	RawJSON         []byte
	OwnerRefs       []ownerRef
	Labels          map[string]string
	// status holds the materialized status columns — see status.go.
	status statusReading
}

type ownerRef struct {
	UID          string
	IsController bool
}

// projectObject flattens an object body into the objects columns, redacting and
// stripping the body along the way.
func projectObject(u *unstructured.Unstructured) (objectRow, error) {
	// A nil body would panic on GetUID inside a worker goroutine, taking the process
	// with it.
	if u == nil || u.Object == nil {
		return objectRow{}, fmt.Errorf("kubestore: object is empty")
	}
	uid := string(u.GetUID())
	if uid == "" {
		return objectRow{}, fmt.Errorf("kubestore: %s has empty UID", u.GetKind())
	}

	rawJSON, err := json.Marshal(sanitize(u).Object)
	if err != nil {
		return objectRow{}, err
	}

	row := objectRow{
		UID:             uid,
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		ResourceVersion: u.GetResourceVersion(),
		Generation:      u.GetGeneration(),
		RawJSON:         rawJSON,
		Labels:          u.GetLabels(),
	}
	if ts := u.GetCreationTimestamp(); !ts.IsZero() {
		row.CreatedAt = ts.UnixMilli()
	}
	// Read the ORIGINAL body, not the sanitized copy, whose redaction and strips could
	// drop a field a reading depends on.
	row.status = extractStatus(u)
	for _, ref := range u.GetOwnerReferences() {
		if ref.UID == "" {
			continue
		}
		row.OwnerRefs = append(row.OwnerRefs, ownerRef{
			UID:          string(ref.UID),
			IsController: ref.Controller != nil && *ref.Controller,
		})
	}
	return row, nil
}

// sanitize returns the body as it should be stored: server noise removed, Secret values
// redacted. It deep-copies because the caller's body is the live watch object.
//
// Stripping is a real saving — managedFields plus the last-applied annotation are
// roughly half a typical object's bytes and nothing reads them. Reads are then pure
// pass-through, which is what lets ClusterCachedDataObject serve raw_json verbatim.
func sanitize(u *unstructured.Unstructured) *unstructured.Unstructured {
	out := u.DeepCopy()
	unstructured.RemoveNestedField(out.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(out.Object, "metadata", "annotations",
		"kubectl.kubernetes.io/last-applied-configuration")
	// An annotations map emptied by that removal is noise of its own.
	if ann, ok, _ := unstructured.NestedMap(out.Object, "metadata", "annotations"); ok && len(ann) == 0 {
		unstructured.RemoveNestedField(out.Object, "metadata", "annotations")
	}
	if isSecret(out) {
		redactSecret(out)
	}
	return out
}

// isSecret reads the BODY's own kind, not the worker's configured one, so redaction
// cannot be bypassed by how the object was addressed.
func isSecret(u *unstructured.Unstructured) bool {
	return u.GetKind() == "Secret" && u.GetAPIVersion() == "v1"
}

// redactSecret strips a Secret's values, keeping its keys, so the cache file never
// holds the cluster's credentials. stringData is write-only server-side, so it is
// dropped.
func redactSecret(u *unstructured.Unstructured) {
	unstructured.RemoveNestedField(u.Object, "stringData")
	data, ok, _ := unstructured.NestedMap(u.Object, "data")
	if !ok {
		return
	}
	for k := range data {
		data[k] = redactedValue
	}
	_ = unstructured.SetNestedMap(u.Object, data, "data")
}

// insertObjectRow upserts one object and rewrites its edges — the chokepoint both the
// watch-delta and relist-page paths route through.
//
// Edges are DELETEd then re-inserted, not upserted: an object that lost a label or an
// ownerReference must lose the row too. Both tables are uid-keyed, so it is a point
// lookup.
func insertObjectRow(ctx context.Context, ex execer, k Kind, row objectRow, now int64) error {
	if err := recordStatusTransition(ctx, ex, row, now); err != nil {
		return err
	}
	rawJSON, err := compressRaw(row.RawJSON)
	if err != nil {
		return err
	}
	_, err = ex.ExecContext(ctx, `
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
		row.UID, k.APIVersion, k.Kind, row.Namespace, row.Name,
		row.ResourceVersion, row.Generation, row.CreatedAt, now, rawJSON,
		nullIfEmpty(row.status.Summary), row.status.Ready, row.status.Total,
		row.status.Restart, nullIfEmpty(row.status.Host),
	)
	if err != nil {
		return err
	}

	// At most two statements per edge table (one DELETE, one multi-row INSERT), not one
	// per row: a 500-object relist page runs in one transaction on the cache's shared
	// writer, where per-row statements would mean thousands.
	if _, err := ex.ExecContext(ctx, `DELETE FROM owner_refs WHERE child_uid=?`, row.UID); err != nil {
		return err
	}
	if len(row.OwnerRefs) > 0 {
		args := make([]any, 0, len(row.OwnerRefs)*3)
		for _, ref := range row.OwnerRefs {
			args = append(args, row.UID, ref.UID, boolToInt(ref.IsController))
		}
		q := `INSERT INTO owner_refs (child_uid, owner_uid, is_controller) VALUES ` +
			valuesPlaceholders(len(row.OwnerRefs), 3) +
			` ON CONFLICT(child_uid, owner_uid) DO UPDATE SET is_controller=excluded.is_controller`
		if _, err := ex.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}

	if _, err := ex.ExecContext(ctx, `DELETE FROM labels WHERE uid=?`, row.UID); err != nil {
		return err
	}
	if len(row.Labels) > 0 {
		args := make([]any, 0, len(row.Labels)*3)
		for key, v := range row.Labels {
			args = append(args, row.UID, key, v)
		}
		q := `INSERT INTO labels (uid, key, value) VALUES ` + valuesPlaceholders(len(row.Labels), 3) +
			` ON CONFLICT(uid, key) DO UPDATE SET value=excluded.value`
		if _, err := ex.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}
	return nil
}

// recordStatusTransition appends to the status timeline only when the summary CHANGED —
// a relist rewrites every row, so an unconditional insert would bury real transitions
// under a copy of the collection each resync. The guard is a NOT EXISTS on the caller's
// transaction rather than a separate read; this is the hottest path in the store. A
// summaryless kind records nothing.
func recordStatusTransition(ctx context.Context, ex execer, row objectRow, now int64) error {
	if row.status.Summary == "" {
		return nil
	}
	_, err := ex.ExecContext(ctx,
		`INSERT INTO status_history(uid, at, summary)
		 SELECT ?, ?, ?
		 WHERE NOT EXISTS (SELECT 1 FROM objects WHERE uid = ? AND status_summary = ?)`,
		row.UID, now, row.status.Summary, row.UID, row.status.Summary)
	return err
}

// cascadeTables are the per-object side tables and their uid column. Every deleter
// clears these before the objects row, so the list lives here once and no deleter can
// silently skip a table.
//
// owner_refs appears TWICE on purpose: a deleted object is both a child (references
// out) and an owner (its children's references in), and with orphan deletion the
// children outlive it, so inbound edges left behind point at a uid that is gone.
var cascadeTables = []struct{ table, uidCol string }{
	{"labels", "uid"},
	{"owner_refs", "child_uid"},
	{"owner_refs", "owner_uid"},
	{"status_history", "uid"},
}

// deleteObjectRow removes one object and its edges; the objects delete fires the
// kind_counts trigger, keeping the per-kind tally exact.
func deleteObjectRow(ctx context.Context, ex execer, uid string) error {
	for _, c := range cascadeTables {
		if _, err := ex.ExecContext(ctx, `DELETE FROM `+c.table+` WHERE `+c.uidCol+`=?`, uid); err != nil {
			return err
		}
	}
	_, err := ex.ExecContext(ctx, `DELETE FROM objects WHERE uid=?`, uid)
	return err
}

// sweepObjects deletes one kind's objects matching an extra predicate plus their edges,
// in a fixed number of statements: the edges go through a subquery on the same
// predicate, so nothing is read back into Go (a 20k-object relist would otherwise issue
// 60k statements while holding the cache's single writer).
func sweepObjects(ctx context.Context, ex execer, k Kind, extraWhere string, extraArgs ...any) (int, error) {
	where := `api_version=? AND kind=?`
	if extraWhere != "" {
		where += ` AND ` + extraWhere
	}
	args := append([]any{k.APIVersion, k.Kind}, extraArgs...)
	sub := `SELECT uid FROM objects WHERE ` + where
	for _, c := range cascadeTables {
		q := `DELETE FROM ` + c.table + ` WHERE ` + c.uidCol + ` IN (` + sub + `)`
		if _, err := ex.ExecContext(ctx, q, args...); err != nil {
			return 0, err
		}
	}
	res, err := ex.ExecContext(ctx, `DELETE FROM objects WHERE `+where, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// valuesPlaceholders builds "(?,?,?),(?,?,?),…" — rows tuples of cols columns.
func valuesPlaceholders(rows, cols int) string {
	tuple := "(" + strings.Repeat("?,", cols-1) + "?)"
	var b strings.Builder
	b.Grow(rows * (len(tuple) + 1))
	for i := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(tuple)
	}
	return b.String()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// nullIfEmpty writes NULL for an empty string, so an absent reading reads as absence
// rather than as a value of "".
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
