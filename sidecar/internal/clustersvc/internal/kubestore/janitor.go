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

// The per-file janitor: what one cache's own tables hold, and how the file hands pages
// back to the OS.
package kubestore

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// vacuumPagesPerSweep bounds the free pages one pass hands back (~8MiB at a 4KiB page). A
// var only so a test can shrink it.
var vacuumPagesPerSweep int64 = 2048

// Retention is what the janitor's own tables hold. Events are absent on purpose: their
// retention is the server's, mirrored by the relist's prune, and the janitor sweeps only
// tables nothing upstream owns.
type Retention struct {
	// StatusHistoryTTL caps the per-object status timeline. A relist rewrites every row
	// and inserts nothing, so volume is small and a week is cheap. Zero keeps everything.
	StatusHistoryTTL time.Duration
	// DeletesTTL caps the deletes log. By age rather than by count, because what it
	// bounds is how stale a reader's cursor may be before it has to diff again. Zero
	// keeps everything.
	DeletesTTL time.Duration
	// Interval is the sweep cadence. Zero runs no janitor at all, which is what a test
	// about anything else opens its manager with.
	Interval time.Duration
}

// DefaultRetention is the production setting.
var DefaultRetention = Retention{
	StatusHistoryTTL: 7 * 24 * time.Hour,
	DeletesTTL:       time.Hour,
	Interval:         5 * time.Minute,
}

// runJanitor sweeps on a fixed interval until ctx is cancelled, surviving an individual
// sweep's failure — the next interval retries. The first sweep runs HERE rather than at the
// open that spawned this: inline it would run a vacuum under m.mu, which Clear holds across
// close → unlink → reopen and Stats holds for its whole measurement.
func runJanitor(ctx context.Context, id string, f *file, ret Retention) {
	sweep(ctx, id, f, ret)

	t := time.NewTicker(ret.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sweep(ctx, id, f, ret)
	}
}

// sweep trims what this cache's own tables hold past their retention. Sweeps are ordinary
// writes, so they serialize with the syncs behind the writer pool's single connection.
func sweep(ctx context.Context, id string, f *file, ret Retention) {
	// A zero TTL is a table this manager was not given a retention for, and the fields are
	// independent — so a partial Retention must leave each unset one alone. Without the
	// guard the cutoff is now, which trims the whole table: for deletes that also raises
	// every kind's mark to the head of the log, invalidating every reader's cursor at once.
	if ret.StatusHistoryTTL > 0 {
		// Unbounded, unlike the vacuum below: `at` carries no index, so this is a full scan
		// — but the table is append-on-change and small by construction, and one statement
		// is cheaper than a page-by-page walk. If it ever stops being small the answer is an
		// index on `at`, not a chunked delete.
		if _, err := f.stmts().exec(ctx, stmtSweepStatusHistory,
			time.Now().Add(-ret.StatusHistoryTTL).UnixMilli(),
		); err != nil {
			slog.Warn("kubestore: status_history sweep failed", "cacheID", id, "err", err)
		}
	}

	if ret.DeletesTTL > 0 {
		if err := trimDeletes(ctx, f, time.Now().Add(-ret.DeletesTTL).UnixMilli()); err != nil {
			slog.Warn("kubestore: deletes sweep failed", "cacheID", id, "err", err)
		}
	}

	// Hand freed pages back to the OS; under auto_vacuum=INCREMENTAL this walks only the
	// freelist. THE FREELIST decides, never what this sweep itself deleted — the writers
	// that actually free pages (a relist's prune, ClearKind, a Remove) do not vacuum, so a
	// rows-deleted gate would strand the file at its high-water mark forever.
	var freePages int64
	if err := f.db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freePages); err != nil {
		slog.Warn("kubestore: freelist_count failed", "cacheID", id, "err", err)
		return
	}
	if freePages == 0 {
		return
	}
	// Bounded: the argument-less form reclaims the whole freelist in one statement, holding
	// the single writer for as long as that takes.
	pages := min(freePages, vacuumPagesPerSweep)
	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(`PRAGMA incremental_vacuum(%d)`, pages)); err != nil {
		slog.Warn("kubestore: incremental_vacuum failed", "cacheID", id, "err", err)
	}
}
