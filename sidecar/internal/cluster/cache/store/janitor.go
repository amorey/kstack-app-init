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
	"fmt"
	"log/slog"
	"time"
)

// Retention controls how long aging tables hold their rows.
//
// Events are deliberately NOT here: their retention is server-mirrored by the Event
// sync (its relist prunes rows the cluster no longer has), so the cache reflects the
// server's current event set rather than a locally-enforced window. The janitor only
// sweeps tables it alone owns.
// vacuumPagesPerSweep bounds how many free pages one janitor pass hands back. The cache has
// a single writer, so a vacuum holds every kind's sync behind it for as long as it runs —
// and the freelist is biggest precisely when that hurts most, right after an events prune
// or a CRD's Forget. At the default 4KiB page this is ~8MiB per pass, and a bigger backlog
// simply drains over the next few.
//
// A var only so this package's tests can shrink it; production never assigns it.
var vacuumPagesPerSweep int64 = 2048

type Retention struct {
	// StatusHistoryTTL caps how long the per-object status transition timeline is
	// retained. Volume is small — objectsync appends only when an object's status summary
	// actually changes, not on every write — so a week is cheap.
	StatusHistoryTTL time.Duration
	// Interval is the sweep cadence. Too frequent wastes write txns; too
	// rare lets the DB grow past the intended bound between sweeps.
	Interval time.Duration
}

// defaultRetention is what the production sidecar uses. Tests exercise
// sweep directly with a custom retention instead of waiting on the ticker.
var defaultRetention = Retention{
	StatusHistoryTTL: 7 * 24 * time.Hour,
	Interval:         5 * time.Minute,
}

// runJanitor sweeps stale rows on a fixed interval. Blocks until ctx is
// cancelled. Survives individual sweep errors — a transient SQL error
// shouldn't tear down the whole janitor, the next interval will retry.
//
// Sweeps run as ordinary writes against the writer pool, so they
// serialize with sync writes via the pool's MaxOpenConns=1. WAL mode
// keeps readers unblocked.
func runJanitor(ctx context.Context, id string, writer *sql.DB, ret Retention) {
	// Sweep once on startup so a freshly-opened DB with stale rows doesn't wait the
	// full interval. Cheap on an empty DB.
	sweep(ctx, id, writer, ret)

	t := time.NewTicker(ret.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sweep(ctx, id, writer, ret)
	}
}

func sweep(ctx context.Context, id string, writer *sql.DB, ret Retention) {
	now := time.Now().UnixMilli()

	// status_history: keyed by the transition time `at`.
	if res, err := writer.ExecContext(ctx,
		`DELETE FROM status_history WHERE at < ?`,
		now-ret.StatusHistoryTTL.Milliseconds(),
	); err != nil {
		slog.Warn("janitor: status_history sweep failed", "id", id, "err", err)
	} else if n, _ := res.RowsAffected(); n > 0 {
		slog.Debug("janitor: trimmed status_history", "id", id, "rows", n)
	}

	// Hand freed pages back to the OS. With auto_vacuum=INCREMENTAL this only walks
	// the freelist, not the whole file.
	//
	// The FREELIST decides, not our own deletions. This sweep trims one small table, while
	// the writers that actually free pages are elsewhere — an events relist prune, a kind's
	// Forget, an object delete sweep — and none of them vacuum. Gating on totalDeleted meant
	// uninstalling a CRD holding 50k objects freed every one of its pages and the file sat
	// at its high-water mark until some unrelated status_history row happened to age out.
	// The count is a page-header read, so asking costs nothing on an idle cluster.
	var freePages int64
	if err := writer.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freePages); err != nil {
		slog.Warn("janitor: freelist_count failed", "id", id, "err", err)
		return
	}
	if freePages == 0 {
		return
	}
	// Bounded, not the whole freelist. The argument-less form reclaims every free page in
	// ONE statement, holding the cache's single writer for the duration — and the freelist
	// is largest exactly after the events prune or a CRD's Forget frees tens of thousands
	// of pages, so the unbounded form stalls every kind's sync at the worst moment. A
	// backlog just takes a few sweeps.
	pages := min(freePages, vacuumPagesPerSweep)
	if _, err := writer.ExecContext(ctx, fmt.Sprintf(`PRAGMA incremental_vacuum(%d)`, pages)); err != nil {
		slog.Warn("janitor: incremental_vacuum failed", "id", id, "err", err)
		return
	}
	slog.Debug("janitor: reclaimed free pages", "id", id, "pages", pages, "remaining", freePages-pages)
}
