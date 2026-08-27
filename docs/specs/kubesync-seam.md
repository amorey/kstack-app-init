---
title: The kubesync seam
scope: sidecar
status: Planned
---

# The kubesync seam

## Goal

Fill the caches. `sidecar/internal/clustersvc/internal/kubesync` discovers what a cluster serves,
mirrors every served kind into that cluster's `kubestore` file, and stands behind an answer about
each one.

This spec fixes the **shape and the seam** — the package's exported surface, what `clustersvc` does
with it, and the invariants both sides hold. The sync internals are sketched only far enough to
show the seam is sufficient.

## The picture

```
clusterCacheController      ──Track/Forget(cacheID, Params)──▶  kubesync.Service
   arms from the pause switch                                        │
   mirrors the kind set into ClusterCachedKind records           │ Acquire(context)
   logs discovery runs to its own `discovery` timeline               ▼
clusterCachedKindController                                 kubeconn.Lease
   logs one kind's runs to its `sync` timeline                       │
Caches().WatchHealth / WatchSyncStatus  ◀──Observations()──          │ OpenOrCreate(cacheID)
                                                                     ▼
                                                                kubestore.Store
```

kubesync knows nothing about records. It speaks cache ids, kube-contexts, server UIDs, and GVRs —
and `clustersvc` translates, which is the layering rule the other leaves already follow.

## Decisions this spec takes

1. **One leaf, not two.** Discovery and per-kind sync live in one package. They share a connection
   claim, an identity gate, an arming contract, and the holds a clear needs; splitting them makes
   each of those a contract between packages.
2. **The seam is cache-scoped.** `clustersvc` arms one subject per cache. kubesync discovers the
   kinds and reports them up; the cache controller mirrors that list into records. Records describe
   what syncs — they do not decide it.
3. **A cache mirrors every kind its cluster serves.** No curated set, no demand-driven arming.
4. **kubesync writes `kind_catalog`.** The discovered set lands on disk through `Store.SyncKinds`,
   leaf to leaf, in one transaction with its fingerprint.
5. **`ClusterCachedCatalog` is deleted** (done). With arming on the cache, the kind list on disk,
   and the verdict on a gauge, nothing was left on it: a spec field relaying what its cache already
   computed, an empty status, and one condition. Its history moves to the cache's own event log
   under `category: "discovery"` — one timeline per cache is what a 1:1 child was standing in for.
6. **`ClusterCachedKind` survives as a timeline anchor.** Sync history is per kind, and
   `category` cannot be that axis — it is a fixed vocabulary the UI branches on, not an identity. So
   each kind keeps an object to hang `category: "sync"` on. It carries identity only: no conditions,
   no verdict, nothing that a flip would rewrite.

## The seam

```go
package kubesync

// Params is what one cache syncs: over which context, and as which server. A context is
// not an identity — it can be re-pointed at another cluster — so ServerUID is what makes
// a discovered kind and a mirrored object belong to this cache.
type Params struct {
	// Subject is the caller's wake token, echoed on every signal and never parsed here.
	// clustersvc passes the cache record's beehive name, so a signal doubles as the requeue.
	Subject     string
	ContextName string
	ServerUID   string
}

// Track arms cacheID's session, or updates it in place when the params move. Forget stops
// it, waits for its workers, and drops its claims; it returns only once nothing can still
// write through that cache's store, which is what a teardown needs.
//
// Arming is the caller's policy, never interest: nothing a reader does re-arms a cache the
// user paused.
func (s *Service) Track(cacheID int64, p Params)
func (s *Service) Forget(cacheID int64)

// KindRef identifies one served kind by the pair that is unique per server: a CRD may reuse
// a built-in's plural under another group.
type KindRef struct{ APIVersion, Resource string }

// Signal names the news that moved. A zero Kind is the cache's own — its kind set, or its
// discovery verdict; otherwise one kind's verdict. Coalescing and keyed, so a fleet syncing
// at once neither loses a cache behind a busier one nor overflows a buffer. The value carries
// nothing: the key is the news, and the reader answers it by re-reading.
type Signal struct {
	Subject string
	CacheID int64
	Kind    KindRef
}

type Subscription = *conflate.Receiver[Signal, struct{}]

func (s *Service) Subscribe() Subscription

// KindObservation is one kind's standing answer, and the identity a record is written from:
// the fields above Reason are exactly ClusterCachedKindSpec's.
type KindObservation struct {
	Kind         kubestore.Kind // APIVersion, Kind, Resource
	Namespaced   bool
	Reason       string
	Message      string
	ObjectCount  int
	LastUpdateAt time.Time // when data last arrived
	LastLiveAt   time.Time // last proof the watch is live (a delta or a bookmark)
}

// CacheObservation is one session's standing answer: the discovery verdict, and one entry per
// kind it has discovered.
type CacheObservation struct {
	CacheID int64
	Reason  string
	Message string
	Kinds   []KindObservation
}

// Read is one cache's whole answer in one critical section; ReadKind is one kind's, so a
// per-kind pass does not scan the slice. Both report false for what has not answered yet.
func (s *Service) Read(cacheID int64) (CacheObservation, bool)
func (s *Service) ReadKind(cacheID int64, k KindRef) (KindObservation, bool)

// Observations is every tracked cache's answer, read in one critical section so a fold sees
// one moment rather than a fleet mid-change.
func (s *Service) Observations() []CacheObservation

// WhileCacheStopped runs fn with cacheID's workers stopped and unable to restart, then resumes
// them from the params it still holds. WhileKindStopped is the same for one kind. A clear is
// two steps — stop, then empty — and a worker resuming into the file between them would write
// deltas into an empty database with no cold list behind them.
//
// The session stays TRACKED throughout: a hold suspends, it does not untrack, so nothing has to
// requeue a record to get the cache syncing again.
func (s *Service) WhileCacheStopped(cacheID int64, fn func() error) error
func (s *Service) WhileKindStopped(cacheID int64, k kubestore.Kind, fn func() error) error

// Holding reports that a clear has cacheID's workers stopped, so the health fold does not read
// a clear as a cache that stopped syncing.
func (s *Service) Holding(cacheID int64) bool

// RestartAll restarts every session's workers in place, off their cookies — what a resume poke
// needs, since a watch that died under a sleeping machine reports nothing.
func (s *Service) RestartAll()

// The lifecycle shape every part has. Stop cancels the sessions and drains them; Close releases
// the claims and the signal hub.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error)
func (s *Service) Close() error
```

Its two dependencies are the narrow interfaces the leaves already hand each other:
`connService{ Acquire(contextName string) kubeconn.Lease }` and
`storeManager{ OpenOrCreate(cacheID int64) (*kubestore.Store, error) }`.

### The reason vocabulary

This leaf's own words; the controllers map them onto event reasons.

| Level | Reason | Means |
| --- | --- | --- |
| cache | `NoConnection` | nothing has reached the server; the session is suspended |
| cache | `IdentityMismatch` | the context's connection does not answer as `ServerUID` |
| cache | `Discovering` | a sweep is running and none has answered yet |
| cache | `Discovered` | a sweep has answered; the kind set is current |
| cache | `DiscoveryFailed` | the sweep failed and is retrying |
| kind | `Syncing` | listing, or watching but not caught up |
| kind | `Watching` | caught up and streaming deltas, proven live |
| kind | `Stale` | caught up, but the watch has stopped proving itself alive |
| kind | `SyncFailed` | the run failed and is retrying |
| kind | `NoConnection` / `IdentityMismatch` | as above, inherited from the session |

## What `clustersvc` does with it

**`clusterCacheController` arms and mirrors.** Its pass already computes `cacheSyncEnabled` and
resolves the cluster above it, so it calls `Track` when the switch holds and `Forget` when it does
not — and `Forget` before `kubestoreMgr.Remove` on a deletion mark. It then reconciles
`Read(cacheID).Kinds` into the `ClusterCachedKind` records it owns: one per kind, created from
the observation's identity fields, and every record no longer in the set deleted. On a discovery
verdict that moved it calls `AddEvent` under `category: "discovery"`.

**`clusterCachedKindController` logs.** Its pass reads `ReadKind` and calls `AddEvent` under
`category: "sync"`. It writes no condition and arms nothing: the record is an anchor for a
timeline, and repeating a run's `(Category, Type, Reason)` extends that run rather than appending,
so a flapping kind costs one row per transition.

**A trigger carries the signals.** `newKubesyncTrigger` maps a `Signal` onto the beehive name it
requeues: a zero `Kind` onto `Signal.Subject`, otherwise onto
`ClusterCachedKindName(CacheID, APIVersion, Resource)`. Pure translation with no state, like
the other two triggers — which is why the record's name is keyed by the cache and the cache's own
name rides the signal.

**The verdicts are gauges, not stored state.** `Caches().WatchHealth` keeps its shape and folds
`Observations()` (plus `Holding`) instead of reading records. A new
`Caches().WatchSyncStatus(clusterID, cacheID)` serves one cache's detail: the discovery reason, and
a row per kind — reason, message, object count, freshness. Both are current-on-subscribe, so
nothing serves a dead process's verdict after a restart.

**The clears wrap.** `Caches().Clear` runs `kubestoreMgr.Clear` inside `WhileCacheStopped`;
`CachedKinds().Clear` runs `Store.ClearKind` inside `WhileKindStopped`.

**Lifecycle order.** kubesync is a `lifecycle.Part` between `kubestore` and `beehive`, so stopping
runs beehive → kubesync → kubestore → kubeconn: no pass can arm a session that is stopping, and no
worker outlives the store it writes. It subscribes to `poke` for `RestartAll`.

### The event timelines

Three, each addressed by an ObjectID and a category — the axis beehive's retention is already
bounded on (`maxEventRuns` per object per category).

| Timeline | Category | Carries |
| --- | --- | --- |
| `Cluster` | `connection` | reachability and identity transitions |
| `ClusterCache` | `discovery` | sweep verdicts, and the kind set changing |
| `ClusterCachedKind` | `sync` | one kind's worker transitions |

A session suspended for `NoConnection` moves the discovery reason on the gauge but writes no
discovery event: that fact is the cluster's, already on its own timeline, and logging it per cache
is the same news twice.

### What this removes

Step 4 has landed: the `ClusterCachedCatalog` kind, its controller, `ensureClusterCachedCatalog`,
the `CachedCatalogs()` family (so `Service` carries four record families), `catalogIDFor`, the
`watchWhenAnchored`/`awaitAnchor`/`drainChanges` dance in `cachedkinds.go`, and
`ClusterCachedKindSpec.Enabled` are all gone. `ClusterCachedKind` is owned by its
`ClusterCache` and named `"cachedkind/{cacheID}/{apiVersion}/{resource}"`, so `WatchByCache` is
`WatchOwnedObjects(cacheID)` and the trigger below can derive a record's name from the cache id.
The schema lost the three `ClusterCachedCatalog` types and `clusterCachedCatalogsWatch`;
`cluster-sync-panel.tsx` lost its fleet-wide catalog subscription and the discovery note it fed.

Names and kinds are persisted, so this was a store migration under the pre-release policy (delete
the dev `beehive.db`).

### The wire and the panel

`ClusterCachedKind` loses `conditions`, and `clusterCacheSyncStatusWatch(id, cacheID)` is new.

`cluster-sync-panel.tsx` takes per-kind verdicts from the new gauge, keeps
`clusterCachedKindsWatch` for the kind→record-id mapping its timeline link needs, and reads
discovery history from `eventsWatch(cacheID, category: "discovery")` while a row is expanded.

## Internal shape

One `session` per tracked cache, holding a `kubeconn.Lease`, one `kubestore.Store` claim for the
session's life, a discovery loop, and a worker per kind.

- **The claim is the session's**, not the worker's, so a tracked cache has a file the moment it is
  armed — which is what `Manager.WatchOpen` readers are waiting on.
- **The identity gate is the session's too.** It waits on `kubeconn.AwaitConnFor(ServerUID)` and
  suspends its workers when the connection stops vouching, rather than each worker re-deciding.
- **Discovery** sweeps on a cadence, woken by the connection bridge and by a watch on the CRDs and
  APIServices that change what a cluster serves. Each answer writes `Store.SyncKinds` (pruning only
  when the sweep reached every group), then diffs the set: new kinds get a worker, departed kinds
  get `Store.ClearKind` and their worker stopped. It drops the `events.k8s.io` spelling of Event —
  one store backs both, and `v1`/`events` is the one that is synced.
- **A worker** is a standing push stream, not a periodic pass: a paginated cold LIST through
  `BeginReplace`/`WritePage`/`Commit` — behind a fleet-wide gate, since arming a cache arms
  hundreds of kinds at once — then a WATCH from the cookie through `ApplyChange`. Events age out
  through `PruneEvents`.
- **A commit publishes** an observation and signals only when the reason moved, so counts and
  timestamps ticking never requeue a record.

## Rules

- **kubesync never names a record.** Cache ids, contexts, server UIDs, GVRs, and one opaque wake
  token it echoes without reading. A record type reaching this package is an import cycle, which is
  the enforcement.
- **Arming is policy, never interest.** Only `Track`/`Forget` start and stop a session, and only
  the cache controller calls them.
- **Nothing syncs into a cache whose connection does not vouch for its `ServerUID`.**
- **A verdict is a gauge; a transition is an event.** Neither is a stored condition — a condition
  would serve a dead process's answer until the passes caught up, and the gauge is the live one.
- **A signal is news, not data.** The key is the whole message; the reader re-reads.
- **A hold suspends, it does not untrack.** Nothing has to requeue a record to undo a clear.
- **`Forget` is synchronous.** It returns only when no worker can still write through that cache.

## Build order

Each step is one red/green cycle and one commit.

1. **The session skeleton**: `Track`/`Forget`/`RestartAll`, the claim and the identity gate, the
   holds, the observation store and the signal bus — with the discovery and sync bodies as seams a
   test substitutes. Nothing above it changes yet.
2. **Discovery**: the sweep, `SyncKinds`, the kind diff, the cache-level reasons.
3. **The kind worker**: cold list, watch, cookie resume, the per-kind reasons and freshness stamps.
4. **Delete `ClusterCachedCatalog`** and re-own `ClusterCachedKind` under its cache, on the
   wire and in the panel. **Done** — a standalone step that removed a kind and changed no behaviour.
5. **The trigger and the two passes**: arming, the record set, the two event timelines.
6. **The gauges and the clears**: `WatchHealth` onto `Observations`, `WatchSyncStatus`, the holds.

Steps 1–3 are `kubesync` alone and land before anything consumes them.

## Not in this pass

- **A curated or demand-driven sync set.** Every served kind is mirrored. A cache that is too
  expensive is a later decision with its own spec, and this seam does not foreclose it: the sync
  set is the session's, not the records'.
- **Per-kind sync toggles.** The pause switch is the cluster's, relayed down whole.
- **Schema JSON for a kind.** `kind_catalog.schema_json` stays unwritten.
- **The kstack event log itself.** `ListEvents`/`WatchEvents` still panic; the three timelines above
  are what they will serve, and filling them in is its own step.

## Done when

Run the app against a real cluster: the dashboard nav fills with that cluster's kinds, a table of
any kind shows its objects and updates as they change, and the health badge reads healthy. Pause
the cluster and the workers stop; resume it and they restart off their cookies without re-listing.
Clear a cache mid-sync and it refills without a restart. Break one kind's RBAC and the sync panel
names that kind, and its timeline still shows the transition after a restart. Suspend the machine
and resume it, and every cache is live again within a poke.

Docs land in the same commits: `sidecar/CLAUDE.md`'s cluster section gains the kubesync leaf and
loses the "nothing fills a cache" notice; the root `CLAUDE.md` loses its copy. Two decisions are owed an ADR — the cache-scoped seam with arming as policy, and records
as timeline anchors rather than status mirrors. Delete this spec when the last step lands.
