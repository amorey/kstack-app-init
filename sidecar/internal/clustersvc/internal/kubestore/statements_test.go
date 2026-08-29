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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every id is compiled at open, so a statement that will not parse fails the cache's open
// rather than the write that first reaches it. The reader takes only the reads: a write
// prepared there would meet query_only at execution, long after the mistake.
func TestOpenFilePreparesEveryStatement(t *testing.T) {
	store := newTestStore(t)
	f, err := store.file()
	require.NoError(t, err)

	for id := range stmtID(numStmts) {
		require.NotEmpty(t, stmtText[id], "id %d has no text", id)
		assert.Equal(t, stmtWrites[id], f.writeStmts[id] != nil,
			"the writer's set disagrees with stmtWrites: %s", stmtText[id])
		assert.Equal(t, !stmtWrites[id], f.readStmts[id] != nil,
			"the reader's set disagrees with stmtWrites: %s", stmtText[id])
	}
}

// stmtWrites is hand-maintained and drives its own enforcement — the reader prepares by
// it and a call routes by it — so a wrong entry is invisible until the statement executes
// and SQLite answers "attempt to write a readonly database". Nothing but the text can say
// what a statement does, so the text is what this checks.
func TestEveryStatementDeclaresWhatItDoes(t *testing.T) {
	for id := range stmtID(numStmts) {
		text := strings.TrimSpace(stmtText[id])
		// WITH counts: a read written as a CTE is still a read, and misfiling one as a
		// write would prepare it on the writer alone and queue it behind the single
		// write connection — the queuing the reader pool exists to prevent.
		reads := strings.HasPrefix(text, "SELECT") || strings.HasPrefix(text, "WITH")
		assert.Equal(t, !reads, stmtWrites[id], "declaration disagrees with the text: %s", text)
	}
}

// A helper serves both a delta (straight on the writer) and a relist page (in a
// transaction), so the value it hangs off carries an optional transaction and rebinds the
// prepared statement onto it. Without the rebinding the write would land on the pool,
// outside the transaction, and survive a rollback.
func TestAStatementIssuedInATransactionRollsBackWithIt(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	f, err := store.file()
	require.NoError(t, err)
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = f.tx(tx).exec(ctx, stmtUpsertMeta, "k", "v")
	require.NoError(t, err)

	// Read back through the raw transaction, not through the helpers: this is a read
	// inside a write transaction, the one combination the routing cannot serve.
	var v string
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT value FROM cluster_meta WHERE key = ?`, "k").Scan(&v))
	require.Equal(t, "v", v)

	require.NoError(t, tx.Rollback())

	_, ok, err := getMeta(ctx, f.stmts(), "k")
	require.NoError(t, err)
	assert.False(t, ok, "the write escaped the transaction")
}

// Beehive's rule, and the one this whole file exists to serve: a text at a call site is a
// text nothing prepared, so it is compiled on every call however constant it looks.
func TestNoSQLTextLivesOutsideTheTable(t *testing.T) {
	// Statements that cannot be prepared here, each with its reason.
	exempt := map[string]string{
		"SELECT COALESCE(SUM(count), 0)": "the stats read also serves a CLOSED cache, " +
			"through a per-call read-only open that has no prepared set",
	}

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go") && fi.Name() != "statements.go"
	}, 0)
	require.NoError(t, err)

	sqlStart := regexp.MustCompile(`^(?i)(SELECT|INSERT|UPDATE|DELETE|WITH)\b`)
	for _, pkg := range pkgs {
		for name, f := range pkg.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				text := strings.TrimSpace(strings.Trim(lit.Value, "`\""))
				if !sqlStart.MatchString(text) {
					return true
				}
				for prefix := range exempt {
					if strings.HasPrefix(text, prefix) {
						return true
					}
				}
				t.Errorf("%s:%d: SQL text outside stmtText: %.60s…",
					filepath.Base(name), fset.Position(lit.Pos()).Line, text)
				return true
			})
		}
	}
}

// The relist prune's cascades used to re-derive the doomed set with the same subquery, so
// the predicate ran once per side table and again for the delete itself.
func TestTheSweepNamesObjectsOnce(t *testing.T) {
	ids := []stmtID{stmtSweepObjects}
	for _, c := range cascadeTables {
		ids = append(ids, c.byUIDs)
	}

	var namesObjects int
	for _, id := range ids {
		if strings.Contains(stmtText[id], "objects") {
			namesObjects++
		}
	}

	assert.Equal(t, 1, namesObjects, "the cascades take the uids the delete returned")
}

// Tx.StmtContext allocates a fresh statement per call, registers it db-wide and releases
// it only at commit — so a 500-object relist page issuing six statements each would pile
// up thousands of wrappers before it lands, on the path the prepared set exists to make
// cheap. One rebinding per id, reused for the transaction's life.
func TestATransactionRebindsEachStatementOnce(t *testing.T) {
	ctx := context.Background()
	f, err := newTestStore(t).file()
	require.NoError(t, err)
	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck // nothing to keep

	st := f.tx(tx)

	assert.Same(t, st.stmt(ctx, stmtUpsertMeta), st.stmt(ctx, stmtUpsertMeta))
}
