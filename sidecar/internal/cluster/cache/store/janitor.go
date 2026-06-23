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
	"log/slog"
	"time"
)

// Retention controls how long aging tables hold their rows.
type Retention struct {
	// EventsTTL caps how long events stay in SQLite after their last_seen
	// timestamp. kube-apiserver GCs events at ~1h by default; we keep
	// longer so the agent can answer "what happened overnight" questions
	// but still discard truly stale data.
	EventsTTL time.Duration
	// StatusHistoryTTL caps how long the per-object status transition
	// timeline is retained. Volume is tiny (only writes when a summary
	// actually changes), so a week is cheap.
	StatusHistoryTTL time.Duration
	// Interval is the sweep cadence. Too frequent wastes write txns; too
	// rare lets the DB grow past the intended bound between sweeps.
	Interval time.Duration
}

// defaultRetention is what the production sidecar uses. Tests exercise
// sweep directly with a custom retention instead of waiting on the ticker.
var defaultRetention = Retention{
	EventsTTL:        24 * time.Hour,
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
	// Run once immediately on startup so a freshly-opened DB that already
	// has stale rows doesn't have to wait the full interval. Cheap on an
	// empty DB.
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
	var totalDeleted int64

	// events: prefer last_seen (server's view) but fall back to
	// updated_at (our ingest time) so an event with missing timestamps
	// still ages out instead of living forever.
	if res, err := writer.ExecContext(ctx,
		`DELETE FROM events WHERE COALESCE(last_seen, updated_at) < ?`,
		now-ret.EventsTTL.Milliseconds(),
	); err != nil {
		slog.Warn("janitor: events sweep failed", "id", id, "err", err)
	} else if n, _ := res.RowsAffected(); n > 0 {
		slog.Debug("janitor: trimmed events", "id", id, "rows", n)
		totalDeleted += n
	}

	// status_history: keyed by the transition time `at`.
	if res, err := writer.ExecContext(ctx,
		`DELETE FROM status_history WHERE at < ?`,
		now-ret.StatusHistoryTTL.Milliseconds(),
	); err != nil {
		slog.Warn("janitor: status_history sweep failed", "id", id, "err", err)
	} else if n, _ := res.RowsAffected(); n > 0 {
		slog.Debug("janitor: trimmed status_history", "id", id, "rows", n)
		totalDeleted += n
	}

	// Hand the freed pages back to the OS. Cheap and bounded — the
	// migration set auto_vacuum=INCREMENTAL so this only walks the
	// freelist, doesn't rewrite the whole file. No-op when nothing was
	// deleted, so we skip the call to save a write txn on idle clusters.
	if totalDeleted > 0 {
		if _, err := writer.ExecContext(ctx, `PRAGMA incremental_vacuum`); err != nil {
			slog.Warn("janitor: incremental_vacuum failed", "id", id, "err", err)
		}
	}
}
