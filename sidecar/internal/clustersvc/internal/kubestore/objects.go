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
	"errors"
	"fmt"

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
// errUnprojectable marks a body the projection cannot turn into a row. Every writer skips one
// rather than failing over it: a body a relist drops is simply missing until the next one, where
// a delta the server replays from its position would be handed back forever.
var errUnprojectable = errors.New("kubestore: unprojectable body")

func projectObject(u *unstructured.Unstructured) (objectRow, error) {
	// A nil body would panic on GetUID inside a worker goroutine, taking the process
	// with it.
	if u == nil || u.Object == nil {
		return objectRow{}, fmt.Errorf("%w: object is empty", errUnprojectable)
	}
	uid := string(u.GetUID())
	if uid == "" {
		return objectRow{}, fmt.Errorf("%w: %s has empty UID", errUnprojectable, u.GetKind())
	}

	rawJSON, err := json.Marshal(sanitize(u).Object)
	if err != nil {
		return objectRow{}, fmt.Errorf("%w: %w", errUnprojectable, err)
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
func insertObjectRow(ctx context.Context, st stmts, k Kind, row objectRow, now int64) error {
	if err := recordStatusTransition(ctx, st, row, now); err != nil {
		return err
	}
	rawJSON, err := compressRaw(row.RawJSON)
	if err != nil {
		return err
	}
	_, err = st.exec(ctx, stmtUpsertObject,
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
	if _, err := st.exec(ctx, stmtDeleteOwnerRefsOfChild, row.UID); err != nil {
		return err
	}
	// Guard, not an optimization: OwnerRefs is built by append, so an unowned object
	// marshals to `null`, and json_each('null') yields one all-NULL row — a NOT NULL
	// violation under STRICT. An *empty* array would expand to nothing; a nil does not.
	if len(row.OwnerRefs) > 0 {
		refs := make([][2]any, len(row.OwnerRefs))
		for i, ref := range row.OwnerRefs {
			refs[i] = [2]any{ref.UID, ref.IsController}
		}
		// Tuples, so the columns come out of the element's positions. Marshalling
		// []ownerRef instead gives an array of objects, where `value ->> 0` is NULL.
		refsJSON, err := json.Marshal(refs)
		if err != nil {
			return err
		}
		if _, err := st.exec(ctx, stmtUpsertOwnerRefs, row.UID, string(refsJSON)); err != nil {
			return err
		}
	}

	if _, err := st.exec(ctx, stmtDeleteLabelsOfObject, row.UID); err != nil {
		return err
	}
	// Same guard: an unlabelled object's Labels is the nil map apimachinery returns, and
	// it marshals to `null` exactly as the ref slice does.
	if len(row.Labels) > 0 {
		labelsJSON, err := json.Marshal(row.Labels)
		if err != nil {
			return err
		}
		// A map marshals to an object, so stmtUpsertLabels reads json_each's own
		// key/value. `value ->> 0` here is not merely empty — it fails with "malformed JSON".
		if _, err := st.exec(ctx, stmtUpsertLabels, row.UID, string(labelsJSON)); err != nil {
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
func recordStatusTransition(ctx context.Context, st stmts, row objectRow, now int64) error {
	if row.status.Summary == "" {
		return nil
	}
	_, err := st.exec(ctx, stmtInsertStatusTransition,
		row.UID, now, row.status.Summary, row.UID, row.status.Summary)
	return err
}

// cascadeTables are the per-object side tables, as the pair of statements that clears
// one. Every deleter walks this list before the objects row, so it lives here once and no
// deleter can silently skip a table.
//
// owner_refs appears TWICE on purpose: a deleted object is both a child (references
// out) and an owner (its children's references in), and with orphan deletion the
// children outlive it, so inbound edges left behind point at a uid that is gone.
var cascadeTables = []struct {
	// byUID deletes one object's rows; byUIDs deletes every row of a bound uid list.
	byUID, byUIDs stmtID
}{
	{stmtDeleteLabelsOfObject, stmtDeleteLabelsOfObjects},
	{stmtDeleteOwnerRefsOfChild, stmtDeleteOwnerRefsOfChildren},
	{stmtDeleteOwnerRefsOwnedBy, stmtDeleteOwnerRefsOwnedByAny},
	{stmtDeleteStatusHistoryOfObject, stmtDeleteStatusHistoryOfObjects},
}

// deleteObjectRow removes one object and its edges; the objects delete fires the
// kind_counts trigger, keeping the per-kind tally exact.
func deleteObjectRow(ctx context.Context, st stmts, uid string) error {
	for _, c := range cascadeTables {
		if _, err := st.exec(ctx, c.byUID, uid); err != nil {
			return err
		}
	}
	_, err := st.exec(ctx, stmtDeleteObject, uid)
	return err
}

// sweepObjects is the relist prune: it deletes one kind's objects left behind by the
// mark, plus their edges, and returns how many objects went.
//
// The objects delete goes first and names the uids it took, so the predicate is evaluated
// once rather than once per side table. Safe in that order because nothing references
// objects — the side tables are uid-keyed with no foreign key — and the caller's
// transaction rolls the whole sweep back on any failure.
func sweepObjects(ctx context.Context, st stmts, k Kind, mark int64) (int, error) {
	rows, err := st.query(ctx, stmtSweepObjects, k.APIVersion, k.Kind, mark)
	if err != nil {
		return 0, err
	}
	// Drained to the end, and Err checked: the delete runs whether or not its rows are
	// read, so a short read yields a short uid list and orphans every side-table row of
	// the objects it never saw — silently, since the delete itself succeeded.
	uids := []string{}
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			rows.Close()
			return 0, err
		}
		uids = append(uids, uid)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, err
	}

	uidsJSON, err := json.Marshal(uids)
	if err != nil {
		return 0, err
	}
	for _, c := range cascadeTables {
		if _, err := st.exec(ctx, c.byUIDs, string(uidsJSON)); err != nil {
			return 0, err
		}
	}
	return len(uids), nil
}

// nullIfEmpty writes NULL for an empty string, so an absent reading reads as absence
// rather than as a value of "".
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
