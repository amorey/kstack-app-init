-- The durable, app-level registry of clusters: one row per cluster record,
-- keyed by the registry-minted record UUID — not the remote cluster's UID,
-- which two records can share. Written only by the kube package's store
-- (internal/kube), whose statements are column-scoped by owner: spec_* +
-- generation belong to user/API mutations, the per-variant status_source_*
-- columns each to their variant's importer, status_sync to the sync
-- controller, the remaining status_* to the cluster controller. Timestamps are unix-millis INTEGERs (NULL = unset);
-- booleans are CHECK-gated 0/1 INTEGERs; spec_source, status_source_*, and
-- status_conditions are JSON (the source union with exactly one variant
-- set, the nullable per-source observation blocks, and the k8s-style
-- condition array). New-record policy lives in the importers, which write
-- every column at mint time.
-- Owned by internal/appdb (the app-level <data-dir>/app.db), migrated by the
-- shared internal/sqlitemigrate runner.
CREATE TABLE clusters (
    id          TEXT PRIMARY KEY,           -- registry-minted record UUID
    name        TEXT,                       -- user display name; NULL = unset
    generation  INTEGER NOT NULL DEFAULT 1, -- bumped on spec change only
    created_at  INTEGER NOT NULL,           -- unix-millis
    archived_at INTEGER,                    -- NULL = not archived
    deleted_at  INTEGER,                    -- NULL = live; set = tombstone

    -- spec: desired state (user/API-owned)
    spec_is_sync_enabled    INTEGER NOT NULL CHECK (spec_is_sync_enabled IN (0, 1)),
    spec_is_active          INTEGER NOT NULL CHECK (spec_is_active IN (0, 1)),
    spec_source             TEXT NOT NULL,      -- JSON ClusterSource union

    -- status: last-known observed state (reconciler-owned)
    status_source_kubeconfig     TEXT,                                                     -- JSON KubeconfigStatus; NULL = not kubeconfig-sourced
    status_server_uid            TEXT,                                                     -- NULL = never probed
    status_server_version        TEXT,
    status_principal_username    TEXT,
    status_last_connected_at     INTEGER,                                                  -- unix-millis; NULL = never connected
    status_conditions            TEXT NOT NULL DEFAULT '[]',                               -- JSON ClusterCondition array
    status_sync                  TEXT                                                      -- JSON ClusterSyncStatus (sync controller's block); NULL = never synced
);
