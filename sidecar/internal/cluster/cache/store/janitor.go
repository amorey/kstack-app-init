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

// vacuumPagesPerSweep bounds the free pages one pass hands back (~8MiB at a 4KiB page).
// The cache has a single writer, so a vacuum blocks every kind's sync — and the freelist
// is biggest exactly when that hurts most, right after an events prune or a Forget. A
// backlog drains over the next few sweeps. A var only so tests can shrink it.
var vacuumPagesPerSweep int64 = 2048

// Retention controls how long the janitor's tables hold their rows. Events are
// deliberately absent: their retention is server-mirrored by the Event sync's relist
// prune, and the janitor sweeps only tables it alone owns.
type Retention struct {
	// StatusHistoryTTL caps the per-object status transition timeline. Volume is small
	// (objectsync appends only on an actual status change), so a week is cheap.
	StatusHistoryTTL time.Duration
	// Interval is the sweep cadence: too frequent wastes write txns, too rare lets the
	// DB grow past its bound between sweeps.
	Interval time.Duration
}

// defaultRetention is the production setting; tests call sweep directly instead.
var defaultRetention = Retention{
	StatusHistoryTTL: 7 * 24 * time.Hour,
	Interval:         5 * time.Minute,
}

// runJanitor sweeps stale rows on a fixed interval until ctx is cancelled, surviving
// individual sweep errors (the next interval retries). Sweeps are ordinary writes, so
// they serialize with sync writes via the pool's MaxOpenConns=1.
func runJanitor(ctx context.Context, id string, writer *sql.DB, ret Retention) {
	// Sweep on startup so a freshly-opened DB doesn't wait a full interval.
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

	if res, err := writer.ExecContext(ctx,
		`DELETE FROM status_history WHERE at < ?`,
		now-ret.StatusHistoryTTL.Milliseconds(),
	); err != nil {
		slog.Warn("janitor: status_history sweep failed", "id", id, "err", err)
	} else if n, _ := res.RowsAffected(); n > 0 {
		slog.Debug("janitor: trimmed status_history", "id", id, "rows", n)
	}

	// Hand freed pages back to the OS; under auto_vacuum=INCREMENTAL this walks only the
	// freelist. The FREELIST decides, not this sweep's own deletions — the writers that
	// actually free pages (events prune, a kind's Forget, an object delete sweep) don't
	// vacuum, so gating on rows deleted here would strand a file at its high-water mark.
	// The count is a page-header read, free on an idle cluster.
	var freePages int64
	if err := writer.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freePages); err != nil {
		slog.Warn("janitor: freelist_count failed", "id", id, "err", err)
		return
	}
	if freePages == 0 {
		return
	}
	// Bounded: the argument-less form reclaims the whole freelist in one statement,
	// holding the single writer — see vacuumPagesPerSweep.
	pages := min(freePages, vacuumPagesPerSweep)
	if _, err := writer.ExecContext(ctx, fmt.Sprintf(`PRAGMA incremental_vacuum(%d)`, pages)); err != nil {
		slog.Warn("janitor: incremental_vacuum failed", "id", id, "err", err)
		return
	}
	slog.Debug("janitor: reclaimed free pages", "id", id, "pages", pages, "remaining", freePages-pages)
}
