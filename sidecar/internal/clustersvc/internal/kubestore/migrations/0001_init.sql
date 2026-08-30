-- NOTE: auto_vacuum=INCREMENTAL (so the janitor's PRAGMA incremental_vacuum can
-- return DELETE'd pages to the OS) is NOT set here. It must run BEFORE any table
-- exists — including the migration runner's own schema_migrations table — so
-- SQLite would silently ignore it from inside a migration. It is on the writer
-- pool's DSN instead (see sqlitemigrate.OpenPool).

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
-- WITHOUT ROWID because the row is barely more than its key: as a rowid table SQLite
-- would store that key a second time in an autoindex. Nothing indexes it, so there is no
-- index growth paying the saving back.
CREATE TABLE cluster_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT, WITHOUT ROWID;

-- The file's write counter, seeded here so nothing has to initialize it at open. Every
-- write transaction takes the next number and stamps everything it writes with it.
INSERT INTO cluster_meta (key, value) VALUES ('seq', '0');

-- One row per Kubernetes object (built-in or CRD). The universal entry point.
--
-- status_summary and the four count/host fields are cross-kind materialized values,
-- written by objectsync at write time (see its status.go). They exist to make the cache
-- QUERYABLE in SQL -- "which pods in this namespace are not ready", "which nodes are
-- NotReady" -- without unpacking a blob per row. They are NOT what the dashboard renders:
-- the frontend derives its per-kind cells client-side from raw_json, which is why the body
-- is served verbatim. Every column is nullable, and a kind with no meaningful reading
-- stores NULL rather than a zero that would read as "none ready". The per-kind meaning:
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
-- zlib-compressed (see kubestore's compressRaw/decompressRaw) — a BLOB, not
-- text. The format is self-identifying (a zlib stream begins with 0x78, plain
-- JSON with '{' = 0x7B), so there is no version prefix yet; one can be added
-- later without a migration.
--
-- A rowid table, unlike the all-key tables here: WITHOUT ROWID wants small rows, and
-- raw_json sitting in overflow pages the identity read never touches is what makes the
-- objects watch's per-ping read cheap.
CREATE TABLE objects (
    uid              TEXT PRIMARY KEY,
    api_version      TEXT NOT NULL,
    kind             TEXT NOT NULL,
    namespace        TEXT NOT NULL DEFAULT '',
    name             TEXT NOT NULL,
    resource_version TEXT NOT NULL,
    generation       INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,    -- object's creationTimestamp
    updated_at       INTEGER NOT NULL,    -- last sync write
    -- Position of the last write that CHANGED the object, off the file's counter. It
    -- moves only when resource_version does, so a relist that rewrites the kind unchanged
    -- leaves every stamp alone. A delete takes the row with it; see the deletes table.
    write_seq        INTEGER NOT NULL,
    status_summary   TEXT,
    ready_count      INTEGER,
    total_count      INTEGER,
    restart_count    INTEGER,
    host             TEXT,
    raw_json         BLOB NOT NULL    -- zlib-compressed object JSON
) STRICT;
CREATE INDEX objects_kind_ns_name ON objects(api_version, kind, namespace, name);
CREATE INDEX objects_kind_host    ON objects(api_version, kind, host);
CREATE INDEX objects_kind_ready   ON objects(api_version, kind, ready_count, total_count) WHERE ready_count IS NOT NULL;
CREATE INDEX objects_ns_kind      ON objects(namespace, api_version, kind);
-- "what moved in this kind past position X" as a range scan, not a scan of the kind.
CREATE INDEX objects_kind_seq     ON objects(api_version, kind, write_seq);

-- Ownership graph (Deployment → ReplicaSet → Pod, Job → Pod, CRD → CRD).
-- Pre-extracted from metadata.ownerReferences so JOINs replace JSON parsing.
--
-- WITHOUT ROWID: the row is its key plus a flag, and ordering the rows by that key puts
-- one child's refs contiguous, so the write path's WHERE child_uid = ? is a single descent
-- of the table rather than an autoindex probe and a rowid fetch per row. owner_refs_owner
-- pays part of it back, carrying the full key where it carried an 8-byte rowid; adding a
-- wide index here is what would turn the trade.
CREATE TABLE owner_refs (
    child_uid     TEXT NOT NULL,
    owner_uid     TEXT NOT NULL,
    is_controller INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (child_uid, owner_uid)
) STRICT, WITHOUT ROWID;
CREATE INDEX owner_refs_owner ON owner_refs(owner_uid);

-- Labels, one row per (object, key). Makes Service-selector resolution
-- a JOIN ("which Pods match these key/value pairs?").
--
-- WITHOUT ROWID, for the reason owner_refs is: a small row keyed by a uid prefix, so
-- rewriting one object's labels is one contiguous descent. labels_kv carries
-- (key, value, uid) where it carried (key, value, rowid).
CREATE TABLE labels (
    uid   TEXT NOT NULL,
    key   TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (uid, key)
) STRICT, WITHOUT ROWID;
CREATE INDEX labels_kv ON labels(key, value);

-- Events. Separate from objects because they have unique columns
-- (first_seen, last_seen, count) and a different access pattern
-- (filtered by involved_uid + time range, very high volume).
-- involved_uid is nullable because some events reference an object by
-- name only (e.g. before the involvedObject's UID is observed).
--
-- Keeps its rowid: events_fts is declared content_rowid='rowid' and its triggers insert
-- new.rowid/old.rowid. A WITHOUT ROWID table has none, so the full-text index would break.
CREATE TABLE events (
    uid           TEXT PRIMARY KEY,
    -- The event's own resourceVersion and the position of the last write that moved it —
    -- the same pair objects carries, so "did it change" is one question on both tables.
    resource_version TEXT NOT NULL,
    write_seq        INTEGER NOT NULL,
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
) STRICT;
CREATE INDEX events_involved        ON events(involved_uid, last_seen DESC);
CREATE INDEX events_kind_ns_name    ON events(involved_kind, involved_ns, involved_name, last_seen DESC);
CREATE INDEX events_last_seen       ON events(last_seen DESC);
CREATE INDEX events_seq             ON events(write_seq);

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
) STRICT;
CREATE INDEX status_history_uid_at ON status_history(uid, at DESC);

-- One entry per row a reader can no longer reach: a deleted object or event, and an object
-- whose identity moved out of the kind it was read under (objects_identity_change_log). A
-- write's position is on the row itself
-- (write_seq); the row is gone by the time a reader learns of a delete, so the uid is kept
-- here. Identity only: the reader holds the row's last-known state and keys the removal by
-- uid. Events log under the fixed ('v1', 'Event') the count triggers use, for the same
-- reason -- the events table conflates both spellings of an event into one row shape.
--
-- The janitor trims by age and records how far per kind, so a cursor at or below its kind's
-- mark can no longer be trusted to have seen every delete above it.
CREATE TABLE deletes (
    seq          INTEGER NOT NULL,       -- the counter write_seq is stamped from
    api_version  TEXT NOT NULL,
    kind         TEXT NOT NULL,
    uid          TEXT NOT NULL,
    at           INTEGER NOT NULL        -- unix millis, for the retention sweep
) STRICT;
CREATE INDEX deletes_kind_seq ON deletes(api_version, kind, seq);

-- Catalog of the kinds this cache holds (built-ins + CRDs). Each kind's sync
-- registers its own row when its worker starts and removes it when the kind stops
-- being synced, so the catalog is "what is mirrored here", not "what the cluster
-- advertises". Lets the agent answer "what kinds exist?" / "is there a CRD called
-- Application?" without re-doing discovery, and surfaces the CRD's OpenAPI schema
-- if available.
--
-- It is also load-bearing for reads: the objects table is keyed by kind, while a
-- watch is opened on the plural resource, so the object reads resolve one to the
-- other through this table. A kind with rows and no catalog row reads as empty --
-- which a changes read reports as such, since "nothing moved" would be a lie.
--
-- A rowid table: schema_json holds a CRD's whole OpenAPI schema, which is the wide row
-- WITHOUT ROWID is wrong for.
CREATE TABLE kind_catalog (
    api_version  TEXT NOT NULL,
    kind         TEXT NOT NULL,
    resource     TEXT NOT NULL,   -- plural lowercase, the URL form ("pods")
    scope        TEXT NOT NULL,   -- "Namespaced" or "Cluster"
    is_crd       INTEGER NOT NULL,
    schema_json  TEXT,            -- OpenAPI v3 schema for CRDs, NULL for built-ins
    -- A CRD's additionalPrinterColumns for THIS version, as the JSON the client renders
    -- from; NULL for a built-in and for a CRD declaring none.
    printer_columns TEXT,
    PRIMARY KEY (api_version, kind)
) STRICT;

-- The plural is the other direction of the same identity, and it must be unique too:
-- the object reads resolve (api_version, resource) back to a Kind through it, and two
-- matching rows would have SQLite answer with an arbitrary index-first one — the
-- kind's table then reads empty forever while its sync is perfectly healthy. Within one
-- api group-version a plural names exactly one Kind, so this is the invariant, not a
-- convenience: a CRD whose Kind is renamed while the sidecar is down would otherwise leave
-- the old row beside the new (the in-process cleanup that drops it needs the previous
-- worker running to know what it was). The upsert clears the loser as it writes
-- (stmtResolveKindRename).
CREATE UNIQUE INDEX kind_catalog_api_resource ON kind_catalog(api_version, resource);

-- Maintained per-kind object counts. The dashboard nav shows a per-kind object
-- count; reading it as a grouped COUNT-join of kind_catalog against the whole
-- objects table would be O(objects) and re-run on every write ping. This table
-- holds the count per (api_version, kind) so that read is O(kinds): a point join,
-- no object scan.
--
-- It is kept SEPARATE from kind_catalog on purpose: these counts are maintained
-- SOLELY by the triggers below, which must work whether or not the kind has a
-- catalog row at that moment. Dropping a kind deletes its catalog row in the same
-- transaction as its objects, and the events triggers key on a hardcoded
-- ('v1','Event') that only the Event sync's own registration puts in the catalog.
-- Keying kind_counts on (api_version, kind) alone keeps it exactly consistent with
-- the objects table within each write transaction, independent of catalog churn.
--
-- WITHOUT ROWID: two key columns and a counter, one row per kind, and no secondary index
-- to pay the saving back.
CREATE TABLE kind_counts (
    api_version TEXT NOT NULL,
    kind        TEXT NOT NULL,
    count       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (api_version, kind)
) STRICT, WITHOUT ROWID;

-- A new object bumps its kind's counter. An update of an existing object goes
-- through INSERT ... ON CONFLICT(uid) DO UPDATE, which fires the UPDATE trigger
-- (below), not this one — so this only ever counts genuinely-new rows.
CREATE TRIGGER objects_kind_count_insert AFTER INSERT ON objects BEGIN
    INSERT INTO kind_counts (api_version, kind, count)
    VALUES (new.api_version, new.kind, 1)
    ON CONFLICT(api_version, kind) DO UPDATE SET count = count + 1;
END;

-- A deleted object (watch delete, ReplaceFull prune, or orphaned-kind eviction)
-- decrements its kind's counter. The row is left at 0 rather than removed — a
-- still-advertised-but-empty kind should read 0, and re-adding is cheaper than
-- churning the row.
CREATE TRIGGER objects_kind_count_delete AFTER DELETE ON objects BEGIN
    UPDATE kind_counts SET count = count - 1
    WHERE api_version = old.api_version AND kind = old.kind;
END;

-- A k8s uid's kind is immutable, so an object update normally leaves the count
-- untouched. This trigger fires only if an object's identity somehow changes
-- (defensive), moving the count from the old kind to the new one.
CREATE TRIGGER objects_kind_count_update AFTER UPDATE ON objects
WHEN old.api_version <> new.api_version OR old.kind <> new.kind BEGIN
    UPDATE kind_counts SET count = count - 1
    WHERE api_version = old.api_version AND kind = old.kind;
    INSERT INTO kind_counts (api_version, kind, count)
    VALUES (new.api_version, new.kind, 1)
    ON CONFLICT(api_version, kind) DO UPDATE SET count = count + 1;
END;

-- The same identity change, logged as a delete under the kind the row LEFT. A reader
-- takes both a kind's rows and its deletes by (api_version, kind), so without this the row
-- is in neither: it stops matching the old kind's rows, and nothing took it away. The
-- position is new.write_seq, which the upsert's CASE moves for exactly this case.
CREATE TRIGGER objects_identity_change_log AFTER UPDATE ON objects
WHEN old.api_version <> new.api_version OR old.kind <> new.kind BEGIN
    INSERT INTO deletes (seq, api_version, kind, uid, at)
    VALUES (new.write_seq, old.api_version, old.kind, old.uid, new.updated_at);
END;

-- Events live in their own table (they have unique columns + a distinct access
-- pattern), so the objects triggers above never count them and the dashboard
-- nav's "Events" kind would read 0 forever. These two triggers maintain its
-- kind_counts row so the badge is accurate. The key is hardcoded ('v1','Event'):
-- the events table conflates core v1 and events.k8s.io/v1 into one row shape (no
-- api_version column of the event itself), and both the Event sync's own
-- kind_catalog row and the webview's curated leaf join on core ('v1','Event') —
-- so all cached events roll into that single key. That catalog row is what makes
-- this count reachable: store.Kinds is a kind_catalog LEFT JOIN, so a count with
-- no catalog entry beside it is invisible. There is no UPDATE trigger: an event's kind
-- never changes, and a re-observed event upserts via ON CONFLICT(uid) DO UPDATE,
-- which fires the UPDATE trigger (absent here), so it correctly leaves the count
-- untouched — only genuinely-new rows increment.
CREATE TRIGGER events_kind_count_insert AFTER INSERT ON events BEGIN
    INSERT INTO kind_counts (api_version, kind, count)
    VALUES ('v1', 'Event', 1)
    ON CONFLICT(api_version, kind) DO UPDATE SET count = count + 1;
END;

CREATE TRIGGER events_kind_count_delete AFTER DELETE ON events BEGIN
    UPDATE kind_counts SET count = count - 1
    WHERE api_version = 'v1' AND kind = 'Event';
END;
