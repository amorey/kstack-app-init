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

// The kind_catalog table: what the cluster serves, as one sweep's answer reconciles it.
package kubestore

import (
	"context"
	"fmt"
	"strings"
)

// The two scopes a kind is served at, as the column spells them.
const (
	ScopeNamespaced = "Namespaced"
	ScopeCluster    = "Cluster"
)

// KindRow is one kind_catalog row, in the table's own vocabulary: the plural and the
// scope as the column spells it, not the caller's bool.
type KindRow struct {
	APIVersion string
	Kind       string
	Resource   string
	Scope      string
	IsCRD      bool
}

// SyncKinds reconciles the whole catalog against one sweep's answer, in one transaction.
// prune deletes the rows the answer did not carry, and is the caller's call: a sweep that
// did not reach every api group has not seen the kinds it is missing.
//
// **This table has one writer.** Rows leave it through the prune alone, so a per-kind
// teardown does not take one with it — the kind is still served, and dropping its row
// would take it out of the nav until the next sweep.
func (s *Store) SyncKinds(ctx context.Context, rows []KindRow, prune bool) error {
	f, err := s.file()
	if err != nil {
		return err
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sync kinds: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	for _, r := range rows {
		// Delete then upsert, because the table has two unique keys and SQLite takes one
		// ON CONFLICT target: a Kind renamed under an unchanged plural conflicts on the
		// index rather than the primary key, which an upsert keyed on the latter cannot
		// resolve. This clears the rename's loser first.
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM kind_catalog WHERE api_version = ? AND resource = ? AND kind <> ?`,
			r.APIVersion, r.Resource, r.Kind); err != nil {
			return fmt.Errorf("sync kinds: resolve %s/%s: %w", r.APIVersion, r.Resource, err)
		}
		// schema_json is deliberately absent from the update: nothing here fills it, and
		// writing NULL on every sweep would make the column unusable to whoever does.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO kind_catalog (api_version, kind, resource, scope, is_crd) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(api_version, kind) DO UPDATE SET
			     resource = excluded.resource, scope = excluded.scope, is_crd = excluded.is_crd`,
			r.APIVersion, r.Kind, r.Resource, r.Scope, r.IsCRD); err != nil {
			return fmt.Errorf("sync kinds: write %s/%s: %w", r.APIVersion, r.Kind, err)
		}
	}

	if prune {
		if err := pruneKinds(ctx, tx, rows); err != nil {
			return fmt.Errorf("sync kinds: prune: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sync kinds: commit: %w", err)
	}
	return nil
}

// pruneKinds deletes every row the answer did not carry. The keep-set is built into the
// statement rather than compared row by row: a catalog is order-of-hundreds, so one pass
// over it costs less than reading it back.
func pruneKinds(ctx context.Context, ex execer, rows []KindRow) error {
	if len(rows) == 0 {
		_, err := ex.ExecContext(ctx, `DELETE FROM kind_catalog`)
		return err
	}
	args := make([]any, 0, len(rows)*2)
	for _, r := range rows {
		args = append(args, r.APIVersion, r.Kind)
	}
	pairs := strings.TrimSuffix(strings.Repeat("(?,?),", len(rows)), ",")
	_, err := ex.ExecContext(ctx,
		`DELETE FROM kind_catalog WHERE (api_version, kind) NOT IN (VALUES `+pairs+`)`, args...)
	return err
}
