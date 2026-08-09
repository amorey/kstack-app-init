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

package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Execer is the subset of *sql.DB / *sql.Tx a write helper needs, so a caller's
// incremental upsert (direct on the writer) and its relist page (inside a transaction)
// can share one statement.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ResumeCookie is one synced collection's list/watch resume position — the
// resourceVersion to seed the next watch from, and when it was written — held as a pair
// of cluster_meta rows. It lives here because cluster_meta is this package's table; the
// sync stores above it (events, per-kind objects) differ only in the key prefix they
// address their pair under, so they share this type rather than the protocol.
//
// Every collection's keys share the "<apiVersion>/<Kind>.<suffix>" namespace.
type ResumeCookie struct {
	cdb   *ClusterDB
	rvKey string
	atKey string
	// now supplies the "written at" stamp in unix milliseconds. A seam only so a caller's
	// tests can freeze it.
	now func() int64
}

// NewResumeCookie builds the cookie for one collection, keyed under prefix (the
// "<apiVersion>/<Kind>" the collection describes itself with). A nil now uses the wall
// clock.
func NewResumeCookie(cdb *ClusterDB, prefix string, now func() int64) *ResumeCookie {
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return &ResumeCookie{
		cdb:   cdb,
		rvKey: prefix + ".last_list_rv",
		atKey: prefix + ".last_list_at",
		now:   now,
	}
}

// Get returns the cookie to seed a watch from, or "" to force a cold full LIST. A
// completed relist's cookie validly resumes even against an empty table — the relist is
// what reconciled it — and a partial pass cleared the cookie on its first written page, so
// a cookie always means "a full LIST completed on disk".
func (c *ResumeCookie) Get(ctx context.Context) (string, error) {
	var v string
	err := c.cdb.Reader().QueryRowContext(ctx,
		`SELECT value FROM cluster_meta WHERE key=?`, c.rvKey).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return v, nil
}

// Persist writes the cookie (rv + timestamp). It takes an Execer so it works both inside a
// relist's commit transaction and directly on the writer for the per-delta advance.
//
// Both rows go in ONE statement, not two upserts. On the commit path that keeps them
// atomic without relying on the caller's transaction; on the per-delta path — which runs
// once per Kubernetes event, beside the row write, on a writer pool with a single
// connection — it halves the statements in the hot loop.
func (c *ResumeCookie) Persist(ctx context.Context, ex Execer, rv string) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?), (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		c.rvKey, rv,
		c.atKey, strconv.FormatInt(c.now(), 10))
	return err
}

// Advance moves the cookie to an object's own resourceVersion, if it has one. This is what
// lets a row and the position that would replay it land in the same transaction.
func (c *ResumeCookie) Advance(ctx context.Context, ex Execer, u *unstructured.Unstructured) error {
	rv := u.GetResourceVersion()
	if rv == "" {
		return nil
	}
	return c.Persist(ctx, ex, rv)
}

// Delete durably removes the cookie so the next start cold-LISTs.
func (c *ResumeCookie) Delete(ctx context.Context, ex Execer) error {
	_, err := ex.ExecContext(ctx, `DELETE FROM cluster_meta WHERE key IN (?, ?)`, c.rvKey, c.atKey)
	return err
}
