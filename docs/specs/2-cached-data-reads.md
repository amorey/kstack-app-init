---
title: Cached data reads
scope: sidecar
status: Planned
---

# Cached data reads

> **Build order — 2.** No prerequisites: the sweep populates `kind_catalog`
> (→ [ADR](../adr/2026-08-26-sweep-writes-the-catalog.md)), so the table has rows. Delivers the
> `CachedData()` family, so the dashboard nav and tables go live. Next:
> [Catalog kinds off disk](3-catalog-kinds-off-disk.md), which consumes the `Kinds(ctx)` read
> built here.

## Goal

Implement the `CachedData()` family — `ListKinds`, `WatchKinds`, `WatchObjects`, `WatchEvents`
(`cacheddata.go`) — over the kubestore: the reads behind `ClusterCache.kinds`,
`clusterCachedDataKindsWatch`, `clusterCachedDataObjectsWatch`, and
`clusterCachedDataEventsWatch`. The wire contract is fixed — the schema, the frame types with
their provenance fields, and the root `CLAUDE.md`'s delta-watch rules — and the served types
already encode the read design (see below). No schema change.

The reads consume the store, not the workers, so tests seed rows directly. **The rows matter more
than they look**: `Objects` resolves a plural through `kind_catalog`, so without them this family
would be structurally complete and answer nothing — which is hard to tell from broken.

## The read design: ping, re-read, diff

**The store's change signal is a coalesced ping, not a row delta, and a watch re-reads and diffs.**
This is `main`'s proven shape (`writeBus` in its `store.go`), and the surviving served types were
built for it — `ClusterCachedDataObject`'s comment says so outright: keeping `RawJSON` in the
struct "is what makes an in-place edit differ across two reads and surface as Modified".

**What that comment gets right is that the diff needs a field an in-place edit moves; what it
names is the expensive one.** `resource_version` is that field, it is already stored, and the
server bumps it on every write — so the objects diff keys on it and `RawJSON` stays on the served
type for the client alone (see the store additions below). The kinds and events diffs have no
comparable column and do compare their whole row, which is why those structs stay comparable.

The loop, shared by all three watches:

1. Subscribe to the bus(es) first.
2. Read the snapshot, emit every row as `Added`, then the `Bookmark`.
3. On each ping, arm a **trailing-edge debounce**. When it fires, re-read and diff by UID against
   the previous snapshot: new UID → `Added`, changed → `Modified`, gone → `Deleted` carrying the
   last-known row. Repeat until ctx ends.

Subscribing before reading closes the only gap; ordering needs nothing more, because a re-read is
always full current state — an early or late ping costs one idempotent re-read, never a wrong
frame. That is why the buses carry no payload and need no coupling to the store's transactions.

**The debounce is load-bearing, and the bus's coalescing is not a substitute for it.** `conflate`
merges only what is *undelivered*, so a reader that drains promptly gets one wake per write, not
one per burst — a rollout on a 5k-Pod cache would turn every watch delta into a full `Objects()`
read and a full-collection diff. `main` ran on the same coalescing bus and still put a
trailing-edge debounce on top (`cache_watch_loop.go`'s `arm`/`disarm`), and it comes over with the
loop. A parameter whose production value is a constant, per the repo's pace-by-parameter rule.

**Three constants, not one**, carried from `main` with its reasoning: `dataKindsDebounce` 250ms,
`dataObjectsDebounce` 250ms, and `dataEventsDebounce` **500ms** — deliberately looser, since
events are the highest-volume stream and the one that storms. (`main`'s fourth,
`defaultCacheStatsDebounce` at 1s, belongs to the stats gauge, which has already landed on its own
cadence.)

**A failed re-read schedules its own retry**, and `main`'s reason transfers verbatim
(`cacheWatchRetryInterval`, 2s, also a parameter): the bus is resource-keyed, so a kind nobody
writes to may not ping for hours, and one transient read error would otherwise leave the client's
table empty indefinitely. Ending the stream with `Stream.Err()` and letting the client reconnect
is the other legal answer and is cheaper here than on `main`, since the boundary already has that
path — but a reconnect re-sends the whole snapshot, so a transient disk error would cost the
client its table. **Retry in place.** That is a different ending from the one a vanished store
gets below: a read that failed still has a file under it, and is worth waiting on.

No poll backstop rides these watches: pull-first is a rule about external sources that can drop
events, and this bus is in-process — a missed ping is a bug to fix, not an environment to survive.
That covers a ping that never arrives, which is a different failure from a read that failed; the
retry above is what covers the second.

## Store additions (`internal/kubestore`)

Carried from `main`'s `store.go`:

- **A reader pool beside the writer** (`sqlitemigrate.OpenPool(path, N)`): the watches re-read on
  every ping, and reads must not queue behind the single-connection writer. It does not replace
  `openReadOnly`, and the two are not interchangeable: the manager's stats path opens a **closed**
  cache's file read-only per call (`countsFromDiskLocked`), while the pool serves an **open** one
  for the life of the file. Say which is which where the pool lands, or the next reader picks
  whichever it finds first.
- **Reads**: `Kinds(ctx)` (the `kind_catalog LEFT JOIN kind_counts`, ordered for stable display —
  `Count` is O(kinds), never an objects scan), `Events(ctx, limit)` (newest window off the
  `last_seen` index), `Objects(ctx, apiVersion, resource)` (whole kind, ordered by
  namespace/name). Their row structs (`KindRow`, `EventRow`, `ObjectRow`) come over with them.
  **`Kinds(ctx)` outlives this spec's own use of it** — [spec 3](3-catalog-kinds-off-disk.md)'s
  fold reconciles its children from that read, so keep it a plain store read with no
  family-shaped assumptions in it. Two traps travel verbatim:
  - The events query's `uid DESC` tiebreak: `last_seen` has one-second resolution, ties straddle
    the limit, and rowid order makes a relist's re-inserted rows read as phantom
    `Deleted`/`Added`.
  - Objects are stored by Kind while the caller holds the plural, so the query translates
    through `kind_catalog` (the schema's unique `(api_version, resource)` index) and rides
    `objects_kind_ns_name`.
- **`rawcodec`** (zlib, landed). **`ObjectRow` carries `ResourceVersion` and the objects diff keys
  on `(uid, resource_version)`, not on the whole struct.** Any server-side write bumps the
  resourceVersion, so it is a sound change key, and it means an unchanged row is never
  decompressed: `Objects` hands back `raw_json` as stored, and the projection to
  `ClusterCachedDataObject` decompresses only the rows that become frames. Whole-struct comparison
  would inflate every body in the collection on every ping — the served type keeps `RawJSON` for
  the *client*, which is a separate question from what the diff keys on.
  **Name the field for what it holds: `ObjectRow.CompressedJSON []byte`**, not `main`'s `RawJSON`,
  which on `main` meant the decompressed body. A projection that forgets to decompress would
  otherwise serve zlib bytes as the JSON scalar, and nothing about the field name would have
  stopped it.
  **This makes a re-read cheaper, not cheap.** `Objects` still reads every compressed body off
  disk on every fire, and the loop still holds the whole compressed collection for the life of the
  subscription — a `Deleted` frame carries the departed row's body, so the previous snapshot has
  to keep it. Reading keys first and fetching bodies only for the changed set would fix that, and
  is **declined here**: it doubles every read into two queries against a moving table for a saving
  we have no measurement of. Revisit with one.
- **The ping buses** (landed): one keyed `objects/<apiVersion>/<resource>`, one `events`. Both end
  when the file closes — including the close inside `Manager.Clear`/`Remove` — which is what ends
  a live watch with a reason. The reader pool joins that close plumbing when it lands, or `Clear`
  leaks reader connections.
- **`Store.Subscribe` grows optional keys** — `Subscribe(keys ...string)`, no keys meaning the
  whole feed. It returns everything today and says the reader filters, so every object write would
  wake every open object watch, which is what `ObjectsKey` and `EventsKey` exist to prevent.
  `conflate` filters **at enqueue** (`Hub.WithKey`, `Hub.WithKeyFilter`), so the key belongs on the
  subscription rather than in the reader's loop. Variadic rather than required, because
  `WatchKinds` wants both buses and there is one hub: for it, every key *is* the filter.
- **A read must not silently follow a `Clear`'s swap.** `Manager.Clear` closes the old file and
  installs a fresh empty one on the same entry (`manager.go`), and `Store` resolves `s.e.file` per
  call — so a `Store` from `StoreIfOpen` follows the swap and a ping still in flight re-reads the
  *new, empty* file. **The store a watch bound to must stop answering when the file under it is
  swapped**: capture the `*file` (or a generation on the entry) at bind, and have a post-swap read
  answer `ErrClosed` like a `Remove` does. `main` got that binding for free — its handle was the
  `*ClusterDB` itself, and a closed one errored rather than resolving to the replacement.
- **The manager read path** (landed): `Manager.StoreIfOpen` returns the open store and never
  creates a file. It has no open-signal yet — the stats gauge re-binds on its own cadence — so a
  reader that must go live the moment a store opens still needs one (`main`'s `Manager.WatchDB`
  shape).

The write side (object upsert/delete, event upsert, the relist prune, the events pruner) has
landed with the sync loop, and so have the two ping buses and the registry read path
(`Manager.StoreIfOpen`, which never creates a file). What is left here is the reads, the row
structs, the reader pool, and the family itself. `kind_catalog`'s writer is the **sweep**
(→ [ADR](../adr/2026-08-26-sweep-writes-the-catalog.md)).

## The family (`cacheddata.go`)

- **Gate, then bind — and a read never creates a file.** Each method resolves the
  (clusterID, cacheID) pair: the cache record must exist and be owned by that cluster; absent or
  mismatched → the `Bookmark` alone (an empty slice for `ListKinds`), per the delta-watch rule —
  definitively empty, never pending, never an error. **That bookmark-then-end shape belongs only
  to a scope that can never be filled**, never to one whose anchor has merely not been created
  yet: that is a wait, and `deltaWatch.pumpChanges` is what serves it. A live pair then **binds to
  the open store or waits for one**, through a registry read path shaped like `main`'s
  `Manager.WatchDB`, reduced to one job: the current store if one is open, plus a publish when one
  opens. No store yet → the `Bookmark` alone, live thereafter (rows arriving after the store opens
  diff from the empty snapshot as ordinary `Added` frames, which the protocol allows).

  **A swap is not a rebind — the stream ends, cleanly.** The publish covers a store that was not
  open yet; a store that goes away under a live watch closes `Frames` with **`Stream.Err()` nil**,
  and the client's reconnect is what re-snapshots against the fresh file. **This is where we
  diverge from `main` deliberately**: it rebound in place, and to do that it emitted one `Deleted`
  per held row first (`emitEmpty` in its `streams.go`). That blanks the client's table for the
  whole gap, where a dropped watch holds last-known data and refills from one snapshot — which is
  what the webview's transport already does with a dropped subscription, and it is the better of
  the two.

  **A clear is a user pressing a button, not a watch breaking, and the error must not say
  otherwise.** A non-nil `Err()` is filed as a watch failure, reaches the client as
  `extensions.watchFailed`, and `subscribe-exchange` takes its terminal path — which calls
  `reportError({ source: 'subscription' })`, popping a UI error **once per open cached-data
  watch** (objects, kinds and events can all be live at once), and sets `watchFailed`, which
  suppresses the backoff reset on the next `open`. A clean end takes the `complete` path instead —
  silent reconnect, generation untouched, last-known data held — which is the same behaviour minus
  the false error. **Reserve `Stream.Err()` for a read that is actually broken**: the retry above
  giving up, or a store shutting down mid-read.

  `Stream`'s own doc argues the opposite default — "a consumer that ignores it turns a broken watch
  into a silent one" — and a reader will meet that comment before this decision. It does not apply
  here because the reconnect *succeeds*: there is no invisible retry loop for an error to make
  visible, and the next snapshot is the report.

  Never an `OpenOrCreate` from a read. The cache teardown deletes the store file on the
  deletion-pending pass, and its correctness argument is that nothing recreates it — a
  reconnecting watcher lands exactly in the mark-to-GC window, where the record still exists and
  any record-gated create would resurrect the file as a permanent orphan; no
  check-record-then-create ordering closes that, because the mark can land between the check and
  the create. Binding only to what exists closes it structurally: **every `OpenOrCreate` belongs to
  something armed by a record** — a `kubesync` worker, or the sweep — and the controller sequences
  both against `Remove` (`ForgetCache` waits for the workers; `Manager.Remove` tombstones the id).
  Because the sweep writes too, an enabled cache has a file from its first sweep rather than from
  its first synced kind, so this bind finds a store sooner.
- **`WatchObjects`** — the loop over `Objects(apiVersion, resource)`, subscribed to that
  resource's key. Frames carry `CacheID` + `APIVersion`/`Resource` (the client's
  straggler-rejection provenance).
  **After a `Clear`, this reads empty rather than pending**, and briefly: the clear wipes
  `kind_catalog` with everything else, and `Objects` resolves the plural through it, so the kind
  is unresolvable until the sweep rewrites the rows. `Caches().Clear` wakes the sweep, so that is
  seconds — but it is the sweep interval if the wake is not delivered, and indefinite for a paused
  cache, whose sweep is disarmed. The view shows "no objects", not a spinner.
- **`WatchEvents`** — the loop over `Events(window)`, subscribed to the events bus; a row that
  ages out of the window diffs as `Deleted` carrying its last-known state. The window is a
  parameter whose production value is the constant (`main`'s `defaultEventsLimit`, 500), doubling
  as the diff window.
- **`WatchKinds`** — the loop over `Kinds()`, keyed by (APIVersion, Resource), subscribed to
  **both** buses: object writes move counts, and event writes move the hardcoded
  `('v1','Event')` count. `IsCRD` reads off the `kind_catalog` row, whose one writer is the
  **sweep** (→ [ADR](../adr/2026-08-26-sweep-writes-the-catalog.md)): rows are the discovered,
  mirrorable kinds, so an advertised kind shows with `Count` 0 before its worker has synced
  anything — what `ClusterCachedDataKind`'s doc promises the nav.
- **`ListKinds`** — one `Kinds()` read, no bus. It is what `ClusterCache.kinds` resolves.
- **Every *watch* returns `*Stream[T]`, not the bare channels the interface holds today** —
  the boundary's own rule: anything reading a fallible upstream returns a `Stream`. `ListKinds` is
  unaffected; it returns a slice. A store that goes away under the watch closes `Frames` with a
  nil `Err()` — a `Clear` is expected, and the client reconnects into the fresh snapshot silently
  (see the gate-then-bind rule above); `Err()` is set only when a read is actually broken. The
  resolvers move from `ptrStream`/`mapStream` to `watchStream` (the watch-failure path), and the
  resolver tests' fake changes shape with the interface. Schema untouched.
- Projection: row structs → the served types in this file (`toCachedDataObject` etc.), unix-millis
  to `time.Time`, zero → the null the field resolvers already map.

Delete each method's `TestUnimplementedBoundaryPanics` entry as it lands.

## Order of work

1. Store reads + row structs + the reader pool, seeded-SQL tests (the two traps above each get a
   pinning test). `rawcodec` has landed with the sync loop.
2. The manager's read path (bind + open-signal) and the close-on-`Clear`/`Remove` semantics. The
   two ping buses have landed.
3. One shared watch loop (subscribe → snapshot → Bookmark → debounce → re-read → diff, with the
   retry), tested over a stand-in row type, the way `deltaWatch` is tested once in
   `stream_test.go`. Both cadences are the loop's parameters, so the tests pick their own
   timescale. Pin the interleaving the swap guard exists for: a ping landing mid-`Clear` must end
   the stream, never diff against the fresh empty store and emit mass `Deleted` frames.
   **The loop cannot be `[T comparable]`**, which is the shape `main`'s had: `ObjectRow` carries
   `CompressedJSON []byte` and so is uncomparable, and `old != new` would not compile. It takes a
   `changed func(old, new T) bool` instead — `(uid, resource_version)` for objects, `==` for kinds
   and events. The key function is already a parameter; this is its pair.
4. The four family methods + `Stream` signature change + resolver rewiring.

When it lands: fold into `sidecar/CLAUDE.md`, rewrite the `CachedData` interface comment in
`service.go` — it still promises "degrades to empty while that cache's db isn't open", which the
bind rule above turns from a degradation into the design — fix `stream.go`'s `Stream` doc, whose
"which is why the Data family does" names this family as the example of a watch that may stay a
plain channel, write the ADR for the read side
(the ping-versus-row-delta reasoning, which the landed
[store ping bus ADR](../adr/2026-08-26-store-change-ping-bus.md) covers for the write side), and
delete this spec.
