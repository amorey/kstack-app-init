---
title: Cached data reads
scope: sidecar
status: Planned
---

# Cached data reads

## Goal

Implement the `CachedData()` family — `ListKinds`, `WatchKinds`, `WatchObjects`, `WatchEvents`
(`cacheddata.go`) — over the kubestore: the reads behind `ClusterCache.kinds`,
`clusterCachedDataKindsWatch`, `clusterCachedDataObjectsWatch`, and
`clusterCachedDataEventsWatch`. The wire contract is fixed — the schema, the frame types with
their provenance fields, and the root `CLAUDE.md`'s delta-watch rules — and the served types
already encode the read design (see below). No schema change.

Implementable ahead of the sync loop: the reads consume the store, not the workers, so tests seed
rows directly and the family lights up the moment workers start writing.

## The read design: ping, re-read, diff

**The store's change signal is a coalesced ping, not a row delta, and a watch re-reads and diffs.**
This is `main`'s proven shape (`writeBus` in its `store.go`) and it is what the surviving served
types were built for — `ClusterCachedDataObject`'s comment says it outright: keeping `RawJSON` in
the struct "is what makes an in-place edit differ across two reads and surface as Modified", and
its string underlying type is what keeps the struct comparable for that diff.

The loop, shared by all three watches:

1. Subscribe to the bus(es) first.
2. Read the snapshot, emit every row as `Added`, then the `Bookmark`.
3. On each ping (coalesced — a burst of writes is one wake), re-read, diff by UID against the
   previous snapshot: new UID → `Added`, struct differs → `Modified`, gone → `Deleted` carrying
   the last-known row. Repeat until ctx ends.

Subscribing before reading closes the only gap; ordering needs nothing more, because a re-read is
always full current state — an early or late ping costs one idempotent re-read, never a wrong
frame. That is why the buses carry no payload and need no coupling to the store's transactions.

No poll backstop rides these watches: pull-first is a rule about external sources that can drop
events, and this bus is in-process — a missed ping is a bug to fix, not an environment to survive.

## Store additions (`internal/kubestore`)

Carried from `main`'s `store.go`, adapted to the registry:

- **A reader pool beside the writer** (`sqlitemigrate.OpenPool(path, N)`): the watches re-read on
  every ping, and reads must not queue behind the single-connection writer.
- **Reads**: `Kinds(ctx)` (the `kind_catalog LEFT JOIN kind_counts`, ordered for stable display —
  `Count` is O(kinds), never an objects scan), `Events(ctx, limit)` (newest window off the
  `last_seen` index), `Objects(ctx, apiVersion, resource)` (whole kind, ordered by
  namespace/name). Their row structs (`KindRow`, `EventRow`, `ObjectRow`) come over with them.
  Two traps travel verbatim:
  - The events query's `uid DESC` tiebreak: `last_seen` has one-second resolution, ties straddle
    the limit, and rowid order makes a relist's re-inserted rows read as phantom
    `Deleted`/`Added`.
  - Objects are stored by Kind while the caller holds the plural, so the query translates
    through `kind_catalog` (the schema's unique `(api_version, resource)` index) and rides
    `objects_kind_ns_name`.
- **`rawcodec`** (zlib, landed): `Objects` decompresses `raw_json` on the way out.
- **The ping buses** (landed): one keyed `objects/<apiVersion>/<resource>`, one `events`. Both end
  when the file closes — including the close inside `Manager.Clear`/`Remove` — which is what ends
  a live watch with a reason. The reader pool joins that close plumbing when it lands, or `Clear`
  leaks reader connections.
- **The manager read path** (landed): `Manager.StoreIfOpen` returns the open store and never creates a
  file. It has no open-signal yet — the stats gauge re-binds on its own cadence — so a reader that
  must go live the moment a store opens still needs one (`main`'s `Manager.WatchDB` shape).

The write side (object upsert/delete, event upsert, the relist prune, the events pruner) has
landed with the sync loop, and so have the two ping buses and the registry read path
(`Manager.StoreIfOpen`, which never creates a file). What is left here is the reads, the row structs,
the reader pool, and the family itself. `kind_catalog`'s writer is the catalog fold
(→ kind-catalog-sync spec).

## The family (`cacheddata.go`)

- **Gate, then bind — and a read never creates a file.** Each method resolves the
  (clusterID, cacheID) pair: the cache record must exist and be owned by that cluster; absent or
  mismatched → the `Bookmark` alone (an empty slice for `ListKinds`), per the delta-watch rule —
  definitively empty, never pending, never an error. A live pair then **binds to the open store
  or waits for one**, through a registry read path shaped like `main`'s `Manager.WatchDB`: the
  current store if one is open, plus a publish when one opens or is swapped. No store yet → the
  `Bookmark` alone, live thereafter (rows arriving after the store opens diff from the empty
  snapshot as ordinary `Added` frames, which the protocol allows).

  Never an `OpenOrCreate` from a read. The cache teardown deletes the store file on the
  deletion-pending pass, and its correctness argument is that nothing recreates it — a
  reconnecting watcher lands exactly in the mark-to-GC window, where the record still exists and
  any record-gated create would resurrect the file as a permanent orphan; no
  check-record-then-create ordering closes that, because the mark can land between the check and
  the create. Binding only to what exists closes it structurally: the worker's `OpenOrCreate` is the
  one creator, and the controller already sequences it against `Delete` (`ForgetCache` waits for
  the workers first).
- **`WatchObjects`** — the loop over `Objects(apiVersion, resource)`, subscribed to that
  resource's key. Frames carry `CacheID` + `APIVersion`/`Resource` (the client's
  straggler-rejection provenance).
- **`WatchEvents`** — the loop over `Events(window)`, subscribed to the events bus; a row that
  ages out of the window diffs as `Deleted` carrying its last-known state. The window is a
  parameter whose production value is the constant (`main`'s `defaultEventsLimit`, 500), doubling
  as the diff window.
- **`WatchKinds`** — the loop over `Kinds()`, keyed by (APIVersion, Resource), subscribed to
  **both** buses: object writes move counts, and event writes move the hardcoded
  `('v1','Event')` count. `IsCRD` reads off the `kind_catalog` row, whose one writer is the
  catalog fold (→ docs/specs/kind-catalog-sync.md): rows are the discovered, mirrorable kinds,
  so an advertised kind shows with `Count` 0 before its worker has synced anything — what
  `ClusterCachedDataKind`'s doc promises the nav.
- **`ListKinds`** — one `Kinds()` read, no bus. It is what `ClusterCache.kinds` resolves.
- **Every method returns `*Stream[T]`, not the bare channels the interface holds today** — the
  boundary's own rule (anything reading a fallible upstream returns a `Stream`), already decided
  in the cached-resource-sync spec. A store closed under the watch (a `Clear`, shutdown) sets
  `Stream.Err()` before `Frames` closes; the client reconnects into the fresh snapshot. The
  resolvers move from `ptrStream`/`mapStream` to `watchStream` (the watch-failure path), and the
  resolver tests' fake changes shape with the interface. Schema untouched.
- Projection: row structs → the served types in this file (`toCachedDataObject` etc.), unix-millis
  to `time.Time`, zero → the null the field resolvers already map.

Delete each method's `TestUnimplementedBoundaryPanics` entry as it lands.

## Order of work

1. Store reads + row structs + `rawcodec` + reader pool, seeded-SQL tests (the two traps above
   each get a pinning test).
2. The ping buses, the registry read path (bind + open-signal), and the
   close-on-`Clear`/`Delete` semantics.
3. One shared watch loop (subscribe → snapshot → Bookmark → re-read → diff), tested over a
   stand-in row type, the way `deltaWatch` is tested once in `stream_test.go`. One interleaving
   to cover deliberately: a ping landing mid-`Clear` can diff against the freshly swapped empty
   store and emit mass `Deleted` frames before the bus close ends the stream — technically
   truthful, and the reconnect re-snapshots, but the tests should pin whichever of the two
   endings they observe rather than assuming one.
4. The four family methods + `Stream` signature change + resolver rewiring.

When it lands: fold into `sidecar/CLAUDE.md`, rewrite the `CachedData` interface comment in
`service.go` — it still promises "degrades to empty while that cache's db isn't open", which the
bind rule above turns from a degradation into the design — extend the cached-resource-sync ADR
(or its own) with the ping-versus-row-delta reasoning, and delete this spec.

## Decided here, amending the sync spec

The cached-resource-sync spec originally specified row-level delta fan-out at the store's
transaction boundary. This spec replaces that with the ping bus: the served types are built for
the re-read diff, the ordering argument dissolves when every read is full state, and `main` ran
this shape. The sync spec's broker section is amended in the same change that adds this file.
