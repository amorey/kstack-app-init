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

// Execer is the *sql.DB / *sql.Tx subset a write helper needs, so an incremental upsert
// (direct on the writer) and a relist page (in a transaction) share one statement.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ResumeCookie is one synced collection's list/watch resume position (resourceVersion +
// write stamp) as a pair of cluster_meta rows keyed under
// "<apiVersion>/<Kind>.<suffix>". The sync stores above differ only in that prefix, so
// they share this type rather than the protocol.
type ResumeCookie struct {
	cdb   *ClusterDB
	rvKey string
	atKey string
	now   func() int64 // unix-millis stamp; a seam so callers' tests can freeze it
}

// NewResumeCookie builds one collection's cookie under prefix ("<apiVersion>/<Kind>");
// a nil now uses the wall clock.
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
// present cookie always means "a full LIST completed on disk" — a partial pass cleared
// it on its first written page, and only Commit rewrites it, so a completed relist
// resumes validly even against an empty table.
// See docs/adr/2026-08-09-kubesync-watch-poll.md.
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

// Persist writes the cookie (rv + timestamp) via an Execer, so it serves both a relist's
// commit transaction and the per-delta advance on the writer. Both rows go in ONE
// statement: atomic without the caller's transaction, and half the statements in the
// per-delta hot loop.
func (c *ResumeCookie) Persist(ctx context.Context, ex Execer, rv string) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?), (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		c.rvKey, rv,
		c.atKey, strconv.FormatInt(c.now(), 10))
	return err
}

// Advance moves the cookie to an object's own resourceVersion, letting a row and the
// position that would replay it land in one transaction.
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
