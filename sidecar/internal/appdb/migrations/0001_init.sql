-- The durable, app-level registry of clusters the sidecar has discovered: one
-- row per cluster, identified by its kube-system namespace UID. Holds the
-- user's enable/disable choice and a few bookkeeping timestamps (unix-millis;
-- 0 means "never"). Small and low-traffic — written only by the clustersync
-- Coordinator via internal/clusterregistry. Owned by internal/appdb (the
-- app-level <data-dir>/app.db), migrated by the shared internal/sqlitemigrate
-- runner.
CREATE TABLE clusters (
    uuid                       TEXT PRIMARY KEY,
    name                       TEXT NOT NULL DEFAULT '',  -- last-known kube-context name
    enabled                    INTEGER NOT NULL DEFAULT 1,
    first_seen_at              INTEGER NOT NULL DEFAULT 0,
    last_synced_at             INTEGER NOT NULL DEFAULT 0,
    last_seen_in_kubeconfig_at INTEGER NOT NULL DEFAULT 0
);
