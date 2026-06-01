-- NOTE: auto_vacuum=INCREMENTAL (so the janitor's PRAGMA incremental_vacuum can
-- return DELETE'd pages to the OS) is NOT set here. It must run BEFORE any table
-- exists — including the migration runner's own schema_migrations table — so
-- SQLite would silently ignore it from inside a migration. It is set on the
-- fresh writer pool just before migrations run (see clustercache.Open).

-- Agent-optimized schema for a per-cluster local cache.
--
-- The shape is informed by what an LLM troubleshooting agent needs:
--   1. One uniform entry point for "any Kubernetes object" — the agent
--      doesn't have to learn a different schema per kind.
--   2. Materialized graph edges (owner_refs, labels) so the agent walks
--      the cluster graph via JOINs instead of parsing JSON.
--   3. Events first-class and joinable by UID.
--   4. A small set of cross-kind materialized fields (status_summary,
--      ready_count, total_count, restart_count, host) that power
--      dashboard list views without per-kind tables.
--   5. Full object preserved as raw_json for deep questions.

-- cluster_meta is a free-form key/value bag for sync bookkeeping
-- (per-kind last LIST resourceVersion + timestamp, etc.) so new
-- metadata doesn't require a migration.
CREATE TABLE cluster_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per Kubernetes object (built-in or CRD). The universal entry
-- point. status_summary and the four count/host fields are cross-kind
-- materialized values populated by the adapter at write time:
--
--   Pod         status: phase or container state ("Running",
--                       "CrashLoopBackOff", "ImagePullBackOff")
--               ready/total: ready vs total containers
--               restart_count: sum across containers
--               host: spec.nodeName
--   Deployment  status: "Available 3/3" / "Progressing 1/3"
--               ready/total: readyReplicas / replicas
--   StatefulSet same as Deployment
--   DaemonSet   ready/total: numberReady / desiredNumberScheduled
--   ReplicaSet  ready/total: readyReplicas / replicas
--   Node        status: "Ready" / "NotReady (DiskPressure)"
--               ready: 1 if Ready=True else 0
--   Service     status: type + cluster_ip
--   PVC         status: phase ("Bound" / "Pending")
--               ready: 1 if Bound else 0
--   Job         status: condition summary
--               ready: 1 if Complete else 0
--   CronJob     status: "<n> active, last run <when>"
--   CRD         status: best-effort from .status.conditions[].type/status
--                       (most CRDs follow the conditions convention)
--
-- raw_json is the full object; kind-specific deep views render from it. Stored
-- zlib-compressed (see clustercache.CompressRaw/DecompressRaw) — a BLOB, not
-- text. The format is self-identifying (a zlib stream begins with 0x78, plain
-- JSON with '{' = 0x7B), so there is no version prefix yet; one can be added
-- later without a migration.
CREATE TABLE objects (
    uid              TEXT PRIMARY KEY,
    api_version      TEXT NOT NULL,
    kind             TEXT NOT NULL,
    namespace        TEXT NOT NULL DEFAULT '',
    name             TEXT NOT NULL,
    resource_version TEXT NOT NULL,
    generation       INTEGER,
    created_at       INTEGER NOT NULL,    -- object's creationTimestamp
    updated_at       INTEGER NOT NULL,    -- last sync write
    status_summary   TEXT,
    ready_count      INTEGER,
    total_count      INTEGER,
    restart_count    INTEGER,
    host             TEXT,
    raw_json         BLOB NOT NULL    -- zlib-compressed object JSON
);
CREATE INDEX objects_kind_ns_name ON objects(kind, namespace, name);
CREATE INDEX objects_kind_host    ON objects(kind, host);
CREATE INDEX objects_kind_ready   ON objects(kind, ready_count, total_count);
CREATE INDEX objects_ns_kind      ON objects(namespace, kind);

-- Ownership graph (Deployment → ReplicaSet → Pod, Job → Pod, CRD → CRD).
-- Pre-extracted from metadata.ownerReferences so JOINs replace JSON parsing.
CREATE TABLE owner_refs (
    child_uid     TEXT NOT NULL,
    owner_uid     TEXT NOT NULL,
    is_controller INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (child_uid, owner_uid)
);
CREATE INDEX owner_refs_owner ON owner_refs(owner_uid);

-- Labels, one row per (object, key). Makes Service-selector resolution
-- a JOIN ("which Pods match these key/value pairs?").
CREATE TABLE labels (
    uid   TEXT NOT NULL,
    key   TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (uid, key)
);
CREATE INDEX labels_kv ON labels(key, value);

-- Events. Separate from objects because they have unique columns
-- (first_seen, last_seen, count) and a different access pattern
-- (filtered by involved_uid + time range, very high volume).
-- involved_uid is nullable because some events reference an object by
-- name only (e.g. before the involvedObject's UID is observed).
CREATE TABLE events (
    uid           TEXT PRIMARY KEY,
    involved_uid  TEXT,
    involved_kind TEXT,
    involved_ns   TEXT,
    involved_name TEXT,
    type          TEXT,           -- Normal / Warning
    reason        TEXT,
    message       TEXT,
    first_seen    INTEGER,
    last_seen     INTEGER,
    count         INTEGER,
    raw_json      BLOB NOT NULL,    -- zlib-compressed event JSON
    updated_at    INTEGER NOT NULL
);
CREATE INDEX events_involved        ON events(involved_uid, last_seen DESC);
CREATE INDEX events_kind_ns_name    ON events(involved_kind, involved_ns, involved_name, last_seen DESC);
CREATE INDEX events_last_seen       ON events(last_seen DESC);

-- Full-text search over event messages/reasons so the agent can ask
-- "anything ImagePullBackOff anywhere?" with one query.
CREATE VIRTUAL TABLE events_fts USING fts5(
    reason, message,
    content='events',
    content_rowid='rowid'
);
CREATE TRIGGER events_fts_insert AFTER INSERT ON events BEGIN
    INSERT INTO events_fts(rowid, reason, message) VALUES (new.rowid, new.reason, new.message);
END;
CREATE TRIGGER events_fts_delete AFTER DELETE ON events BEGIN
    INSERT INTO events_fts(events_fts, rowid, reason, message) VALUES('delete', old.rowid, old.reason, old.message);
END;
CREATE TRIGGER events_fts_update AFTER UPDATE ON events BEGIN
    INSERT INTO events_fts(events_fts, rowid, reason, message) VALUES('delete', old.rowid, old.reason, old.message);
    INSERT INTO events_fts(rowid, reason, message) VALUES (new.rowid, new.reason, new.message);
END;

-- Timeline of status_summary transitions per object. Lets the agent
-- answer "when did this Pod start CrashLooping?" without log-scraping.
-- Only inserted when summary actually changes — no row per sync write.
-- A plain rowid table (no (uid, at) primary key) so two distinct
-- transitions landing in the same millisecond both survive; consecutive
-- duplicate summaries are already filtered out by the writer.
CREATE TABLE status_history (
    uid     TEXT NOT NULL,
    at      INTEGER NOT NULL,
    summary TEXT NOT NULL
);
CREATE INDEX status_history_uid_at ON status_history(uid, at DESC);

-- Catalog of every Kind discovered on the cluster (built-ins + CRDs).
-- Populated at startup from /apis discovery. Lets the agent answer
-- "what kinds exist?" / "is there a CRD called Application?" without
-- re-doing discovery, and surfaces the CRD's OpenAPI schema if available.
CREATE TABLE kind_catalog (
    api_version  TEXT NOT NULL,
    kind         TEXT NOT NULL,
    resource     TEXT NOT NULL,   -- plural lowercase, the URL form ("pods")
    scope        TEXT NOT NULL,   -- "Namespaced" or "Cluster"
    is_crd       INTEGER NOT NULL,
    schema_json  TEXT,            -- OpenAPI v3 schema for CRDs, NULL for built-ins
    PRIMARY KEY (api_version, kind)
);
