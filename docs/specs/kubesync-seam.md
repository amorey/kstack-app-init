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
clusterCacheController      ──TrackDiscovery/ForgetDiscovery──▶  kubesync.Service
   arms from the pause switch                                        │
   mirrors kind_catalog into ClusterCachedKind records               │ Acquire(context)
   logs discovery runs to its own `discovery` timeline               ▼
clusterCachedKindController ──TrackKind/ForgetKind────────────▶  kubeconn.Lease
   logs one kind's runs to its `sync` timeline                       │
Caches().WatchHealth / WatchSyncStatus  ◀──the getters───────        │ OpenOrCreate(cacheID)
                                                                     ▼
                                                                kubestore.Store
```

kubesync knows nothing about records. It speaks cache ids, kube-contexts, server UIDs, and GVRs —
and `clustersvc` translates, which is the layering rule the other leaves already follow.

## Where the records stand

**`ClusterCachedCatalog` is gone; `ClusterCachedKind` stays.** Below `ClusterCache` there is one
record per mirrored kind, owned by the cache, named `cachedkind/{cacheID}/{apiVersion}/{resource}`,
and carrying identity alone — the pause switch is the cache's and is never relayed down.

It earns that place twice over. It is the ObjectID a per-kind `category: "sync"` timeline hangs
off, and `category` cannot be that axis itself: it is a fixed vocabulary the UI branches on, not an
identity. And its pass is what arms that kind's worker, per decision 2 below.

Two consequences the plan rests on:

- **A cache id addresses every record below it.** The cache is woken by its id, and
  `ClusterCachedKindName(cacheID, …)` names each kind's record, so both triggers are pure
  translation from what kubesync already holds.
- **Desired and actual are separate.** `kind_catalog` is what the cluster serves; what has a
  worker is what a record armed. The mirror pass is what closes the gap, and a record is
  the only thing that turns one into the other.

## Decisions this spec takes

1. **One leaf, not two.** Discovery and per-kind sync live in one package. They share a connection
   claim, an identity gate, an arming contract, and the holds a clear needs; splitting them makes
   each of those a contract between packages.
2. **Discovery is cache-scoped; a kind's mirror is armed by its own record.** The cache arms one
   sweep, which reports up what the cluster serves — the desired set. The cache controller mirrors
   that into `ClusterCachedKind` records, and each record's pass arms its own worker. So kubesync
   decides what *exists* and the records decide what is *mirrored*, which is the shape the rest of
   the subsystem already has: `kubeconn` finds a serverUID, the cluster pass creates the cache, the
   cache pass arms the sweep.

   **The two levels AND rather than nest.** `TrackKind` registers that a kind should be mirrored
   and kubesync keeps that registration while its cache is unarmed; `TrackDiscovery`/
   `ForgetDiscovery` decide whether any of it runs. Pausing is one call and resuming is one call,
   with no record written and none requeued — where gating through the records would mean relaying
   the switch onto hundreds of them or walking two hops to the cluster on every kind's pass.
3. **A cache mirrors every kind its cluster serves.** No curated set, no demand-driven arming.
   Events are one kind among them: an events collection lists, watches and relists like any other,
   and the one difference — which table its rows land in — is `kubestore`'s, behind
   `Kind.isCoreEvents()`.
4. **kubesync writes `kind_catalog`.** The discovered set lands on disk through `Store.SyncKinds`,
   leaf to leaf, in one transaction with its fingerprint.

## The seam

```go
package kubesync

// Params is what one cache syncs: over which context, and as which server. A context is not an
// identity — it can be re-pointed at another cluster — so ServerUID is what makes a discovered
// kind and a mirrored object belong to this cache.
type Params struct {
	ContextName string
	ServerUID   string
}

// TrackDiscovery arms cacheID's sweep, or updates it in place when the params move;
// ForgetDiscovery stops it and returns only once nothing can still write through that cache's
// store, which is what a teardown needs.
//
// **It is also what supplies the cache.** Params is what every worker dials over and the lease and
// store claim are taken here, so a kind tracked against a cache with no discovery has nothing to
// run on: this pair says whether a cache syncs at all, and TrackKind below says which kinds.
//
// Arming is the caller's policy, never interest: nothing a reader does re-arms a cache the user
// paused.
func (s *Service) TrackDiscovery(cacheID int64, p Params)
func (s *Service) ForgetDiscovery(cacheID int64)

// Every kind the seam takes is a kubestore.Kind — the same value the caller already holds, the
// same one kubestore writes rows by. There is no second, narrower name for one.
//
// **It is keyed by (APIVersion, Resource), and the singular is data.** That pair is what the
// server guarantees unique per group-version; the plural alone is not enough, since a CRD may
// reuse a built-in's under another group. A Kind renamed under an unchanged plural is the same
// collection, so keying on the singular too would read it as two — a lookup missing a worker that
// is running, news that no longer coalesces with its own past. Every map and bus key inside drops
// it. (kubestore.SyncKinds deletes before upserting for the same rename.)
//
// A worker is armed with the whole value — the plural to open the watch on and the singular the
// rows are keyed by, and nothing else. Not the scope: one Dynamic call watches both,
// each body carries its own namespace, and what renders a kind as namespaced comes off
// kind_catalog.
//
// **The singular cannot be learned from a body.** A collection that emptied while nothing was
// watching lists zero items, and the relist prunes by (api_version, kind) — so a worker that
// waited to read it off an object would sweep nothing and leave every stale row behind.

// TrackKind registers that one kind should be mirrored into cacheID, or updates its shape in
// place; ForgetKind withdraws that and waits for the worker.
//
// The registration OUTLIVES its cache being forgotten, which is what makes a pause one call: a
// cache nobody has armed runs nothing, and arming it again starts every kind still registered,
// with no record written and none requeued. A kind registered against a cache that is not armed
// is held rather than refused, so the record's pass and the cache's may land in either order.
func (s *Service) TrackKind(cacheID int64, k kubestore.Kind)
func (s *Service) ForgetKind(cacheID int64, k kubestore.Kind)

// The supervisor's bookkeeping, aliased rather than copied so an Observation carries exactly what it
// recorded — the same aliases kubeconn declares, for the same reason. Reason stays this package's
// vocabulary; the supervisor treats it as opaque.
type (
	Observation[T any] = supervisor.Observation[T]
	Attempt            = supervisor.Attempt
	Attempts           = supervisor.Attempts
	Verdict            = supervisor.Verdict
	Reason             = supervisor.Reason
)

// DiscoveryState is the sweep's standing answer for one cache: a verdict, and the three reads
// behind it. Flat, as kubeconn.State is over its five probes.
//
// **It carries nothing a sweep found.** The kinds go to kind_catalog and are read back from
// there, and which group-versions a cluster serves is that table's business too. What is left is
// how discovery is DOING.
//
// Each read is accounted for on its own, so one failing says so without dragging the others'
// verdict with it, and Observation carries the supervisor's own record — the last attempt's verdict,
// reason and message, the next attempt, the failure streak — so nothing here restates it.
// (Attempts.InFlight() reads false until the supervisor publishes when a run BEGINS rather than only
// after a pass: the gap TODO.md records against clusterScheduleWatch.probing.)
//
// The fan-out's legs get no field of their own: they are discovered at runtime and differ per
// cluster, where a probe is registered by name and fixed, which is why Resources accounts for all
// of them together.
type DiscoveryState struct {
	// Reason and Message are the whole sweep's verdict, and the one thing here not readable off
	// a probe: which of them decides is a PRECEDENCE rule — a suspended session over a failing
	// read, a failing read over one that has yet to answer. It is made here because the news
	// feed has to gate on it either way, and a boundary folding its own would fold it
	// differently.
	Reason  string
	Message string

	// What each read commits is what the next one needs, and no more. Resources commits the
	// FINGERPRINT it wrote to kind_catalog rather than the catalog itself: the rows belong on
	// disk, and a fingerprint is all "the answer moved" requires.
	APIVersions Observation[[]string] // GET /api — the core group's versions
	APIGroups   Observation[[]string] // GET /apis — the group-versions served, the fan-out's input
	Resources   Observation[uint64]   // the fan-out — the catalog fingerprint it committed
}

// KindState is one mirror's standing answer — a live stream's, where DiscoveryState is a set of
// passes, and the difference decides every field here. It carries no identity: the caller named
// the kind to read it.
//
// A pass is judged by its last attempt. A stream is judged by whether it is established now and
// has recently proven itself alive, which is a different question and needs different evidence:
//
//   - **Silence is ambiguous.** A watch sending nothing is either a quiet collection or a wedged
//     connection. LastLiveAt is what separates them — a delta OR a bookmark, since bookmarks
//     exist to make an idle watch prove itself — and Stale is what it reads as once that proof
//     ages past the threshold.
//   - **A healthy stream has no next attempt.** NextRetryAt is set only while a run is down,
//     where a pass always has one scheduled.
//   - **Flapping hides at an instant.** A watch that reconnects every thirty seconds reads
//     Watching whenever it is read; Restarts is the only field that says otherwise.
type KindState struct {
	Reason  string
	Message string
	// SinceAt is when Reason last moved — "watching since 10:02", which is what a stream has
	// instead of a last-attempt stamp.
	SinceAt time.Time

	// LastUpdateAt is when data last arrived; LastLiveAt the last proof the stream is live,
	// which is the later of the two and the only one that distinguishes idle from wedged.
	//
	// No row count: kind_counts is trigger-maintained, so one here would be a stale copy of a
	// number the database keeps authoritatively, bought with a query per commit. A consumer
	// reads Store.Kinds, which joins every kind's count in one go.
	LastUpdateAt time.Time
	LastLiveAt   time.Time

	// Restarts counts runs this worker has begun without settling; NextRetryAt is when a run
	// that is down will be tried again, and zero while one is up.
	Restarts    int
	NextRetryAt time.Time
}

// The two reads, one per worker, each answered in one critical section. Both report false for
// what has not answered yet — a cache nobody has armed, or a kind whose worker has committed
// nothing.
func (s *Service) GetDiscoveryState(cacheID int64) (DiscoveryState, bool)
func (s *Service) GetKindState(cacheID int64, k kubestore.Kind) (KindState, bool)

// One news feed per worker, because their consumers are two beehive triggers and a trigger wakes a
// record for every value its feed carries — one feed carrying both would wake a cache for each of
// its hundreds of kinds. Each is keyed by exactly what its consumer must address and carries
// nothing else: the key is the news, and the reader answers it by re-reading. Both coalesce per
// key, so a fleet syncing at once neither loses a cache behind a busier one nor overflows a
// buffer.
//
// **News is not a status.** A reader answers it by calling a getter, which is what makes an early
// or late delivery cost an idempotent read rather than a wrong frame; a feed carrying the status
// would have to decide what a dropped frame means.

// DiscoveryNews is keyed by cache id, the whole address of the record it wakes.
type DiscoveryNews = *conflate.Receiver[int64, struct{}]

// KindKey names one kind in one cache — the whole key of a kind's news, and everything a caller
// needs to address the record standing for it. It embeds the same value the methods take, so a
// caller composes one from what it already holds (note key.Kind.Kind is the singular; key.Kind is
// the embedded value).
//
// Carrying the singular is safe HERE, where a lookup would not tolerate it: a rename splits one
// key into two, and both translate to the same record name, so the cost is a duplicate wake and
// never a missed one.
type KindKey struct {
	CacheID int64
	kubestore.Kind
}

type KindNews = *conflate.Receiver[KindKey, struct{}]

func (s *Service) WatchDiscoveryNews() DiscoveryNews
func (s *Service) WatchKindNews() KindNews

// RestartAll restarts every armed worker in place, off its cookie — what a resume poke needs,
// since a watch that died under a sleeping machine reports nothing.
func (s *Service) RestartAll()

// The lifecycle shape every part has. Stop cancels the workers and drains them; Close releases the
// claims and the news hubs.
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
| discovery | `NoConnection` | nothing has reached the server; the session is suspended |
| discovery | `IdentityMismatch` | the context's connection does not answer as `ServerUID` |
| discovery | `Discovering` | a sweep is running and none has answered yet |
| discovery | `Discovered` | a sweep has answered; the kind set is current |
| discovery | `Partial` | the fan-out committed what it read, and some group-versions did not answer |
| discovery | `DiscoveryFailed` | the sweep failed and is retrying |

Discovery carries no `Watching` or `Stale`: there is no watch behind it to prove live, so
`Discovering`/`Discovered` are what `Syncing`/`Watching` are for a collection that can only be
listed.

`Partial` is the fan-out's own verdict and exists because neither of its neighbours is honest
about a sweep where twelve of fourteen legs worked: `Discovered` would reset the backoff ladder
over an aggregated API that is down, and `DiscoveryFailed` would climb it while most of the
catalog refreshed fine. Which legs failed and why is the probe's `Message`; nothing on the seam
carries them structurally until something needs to branch on them.
| kind | `Syncing` | cold-listing a kind with nothing cached |
| kind | `Resyncing` | reconciling a kind that HAS rows against a fresh list, because the position they were current at is one the server no longer serves from. Its own verdict because the rows are served throughout, where `Syncing` has nothing to serve |
| kind | `Resuming` | re-establishing from a cookie, and slow enough to be worth saying |
| kind | `Watching` | caught up and streaming deltas, proven live |
| kind | `Stale` | caught up, but the watch has stopped proving itself alive |
| kind | `SyncFailed` | the run failed and is retrying |
| kind | `NoConnection` / `IdentityMismatch` | as above: the session suspends every worker under it, and each reports its own |

## What `clustersvc` does with it

**`clusterCacheController` arms and mirrors.** Its pass already computes `cacheSyncEnabled` and
resolves the cluster above it, so it calls `TrackDiscovery` when the switch holds and `ForgetDiscovery`
when it does not — and `ForgetDiscovery` before `kubestoreMgr.Remove` on a deletion mark. It then reconciles
the cache's `kind_catalog` into the `ClusterCachedKind` records it owns — woken by the same
discovery news that brings it the verdict. The store's `KindsKey` bus carries the catalog half of
that and nothing else — a cluster going unreachable writes no rows — so a controller listening
there would log only the transitions that coincided with a write, and miss every failure. The feed
is owed for the timeline either way, and one pass converges both — a set reconcile, since
`ClusterCachedKindName(cacheID, apiVersion, resource)` is the dedup key and needs no per-child
bookkeeping:

- **A row with no record** is a `CreateOrUpdate` owned by the cache. Not the `GetOrCreate` that
  creates a cache, whose name *is* its whole spec: a kind's spec carries data outside its name —
  the singular and `Namespaced` — so a renamed or re-scoped kind must converge in place rather
  than be recreated under the name it already holds.
- **A record with no row** is a `Delete`, which marks it; the record's own pass then unwinds it.
- **A cache going away deletes none of them.** They are owned by it, so beehive's GC cascades, and
  the cache's pass has already removed the file every one of them would have cleared.

The desired set comes off disk, never off the seam: `kubestoreMgr.OpenExisting(cacheID)`, then
`Store.KindsWithFingerprint`, then `Release`. Three properties of that read are what the pass rests
on:

- **`OpenExisting` never creates a file**, so a pass that runs before any sweep finds no store
  rather than a fresh empty one — and reads no rows, so it prunes nothing.
- **Its `ok` is the "never swept" bit.** A table with no fingerprint has never had an answer
  written to it, which is not the same as a cluster that serves nothing, and only the first of
  those may delete records.
- **Rows and fingerprint come out of one read transaction**, which is why it is not
  `Store.Kinds`. Read from two snapshots, a stale fingerprint can pass its check while the rows
  beside it are a clear's empty table — and the pass would delete every record for the cache.

Each row maps to a spec directly: `APIVersion`, `Kind`, `Resource`, and `Scope` as `Namespaced`.
`KindRow.Count` is ignored here — it belongs to the stats gauge, not to what is mirrored.

Nothing here has to reason about a failed sweep: the pass reads the table, not
kubesync's answer, and a partial sweep is a table that kept its rows. On a `DiscoveryState` reason
that moved it calls `AddEvent` under `category: "discovery"`.

**`clusterCachedKindController` arms and logs.** Its pass calls `TrackKind` with the three
identity fields off its own spec, then reads `KindState` and calls `AddEvent` under
`category: "sync"`.

On a deletion mark it unwinds in one order and only that one: `ForgetKind`, which returns once the
worker is done, then `Store.ClearKind` to drop the rows it wrote. Clearing first would race a
relist page landing behind it. A record whose cache is already gone skips both — `cacheIDForKind`
reads no owner, and the file went with the cache. It writes no condition: the verdict is
the gauge's, and repeating a run's `(Category, Type, Reason)` extends that run rather than
appending, so a flapping kind costs one row per transition.

**Two triggers carry the news**, one per registration, since a trigger is declared per kind and
wakes a record for every value it reads. They requeue differently because the two records are
addressed differently, and each takes the cheaper handle: the cache's is a `WithTriggerByID` whose
values are cache ids as they come off the feed, and the kind's is a `WithTriggerByName` mapping a
`KindKey` onto `ClusterCachedKindName(CacheID, APIVersion, Resource)` — by name because a kind
record's id is the store's to assign, where its name is derivable from the GVR kubesync syncs. Both stay pure translation
with no state, which is what keeps a record id out of kubesync — an id it was handed is not one it
has to resolve.

**The verdicts are gauges, not stored state.** `Caches().WatchHealth` keeps its shape and folds
the getters per cache instead of reading records — it already loops the cache records to resolve
each one's cluster, so there is no fleet-wide read to add. Both gauges iterate a cache's
`ClusterCachedKind` records — the kinds actually mirrored, which is what a sync gauge reports —
and pluck each one's verdict with `GetKindState`. A new
`Caches().WatchSyncStatus(clusterID, cacheID)` serves one cache's detail: the discovery reason, and
a row per kind: its verdict and freshness from `GetKindState`, and its row count from one
`Store.Kinds` per tick — never from kubesync, which knows only the caches it has armed, where
`kubestore` answers for a paused one too. Both are current-on-subscribe, so
nothing serves a dead process's verdict after a restart.

Neither subscribes to kubesync. Both re-read on `gaugeCadence`, because their counts and freshness
stamps move with no reason change — which is the only thing kubesync signals on — so a gauge
waiting for a signal would go quiet exactly while it is healthy.

**The clears are unresolved.** See the open item below; they land in step 5 either way.

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

### The wire and the panel

`ClusterCache.events` exists and carries both categories above, pending the open question on the
per-kind timeline. Its doc needs a correction either way: it offers `"sync"` as its example
category where the cache's own timeline is `discovery`.

What is added: `clusterCacheSyncStatusWatch(id, cacheID)`, the per-kind verdict gauge. Nothing on
the wire carries a per-kind verdict today — the record that would have is gone — so this is the
only thing that can serve one.

`cluster-sync-panel.tsx` is the one consumer. It takes per-kind verdicts from the new gauge instead
of the rollup's `unhealthyKindRefs`, and reads discovery history from
`eventsWatch(cacheID, category: "discovery")` while a row is expanded. Its sync timeline currently
reads `eventsWatch(cacheID, category: "sync")`; whether that stays cache-scoped is the open
question above.

## The catalog is the kind set

`kind_catalog` in the cache's own file is the one place the set of served kinds lives. The sweep
writes it through `SyncKinds`; the mirror pass reads it back and converges records onto it; the nav
reads it through `CachedData().WatchKinds`. Nothing holds a second copy.

That is not tidiness — it is what makes three rules stop being rules and start being properties of
the table:

- **A partial sweep keeps what it could not confirm.** `SyncKinds` upserts what the sweep read and
  prunes only when told to, so a group that failed leaves its rows standing. Held in memory
  instead, the same behaviour is a merge someone has to write and keep correct, and getting it
  wrong deletes a group's records for the length of an outage and resyncs every one afterwards.
- **A restart has an answer immediately.** The mirror pass reads the catalog and converges before
  any sweep has run, so records survive a restart against an unreachable cluster.
- **"Never swept" is distinguishable from "serves nothing."** The fingerprint is recorded with the
  rows, so its absence — not an empty table — is what tells the mirror pass to prune nothing.

The two paths into the table stay as they were: one writer, the sweep, and readers that never
create a file (`OpenExisting`).

## Internal shape

```
kubesync/
  service.go    the seam: arming and the gate between its two levels, the reads, the news
                feeds, the lifecycle
  session.go    one cache: its lease, its store claim, the identity gate, suspend/resume, and
                the set of workers under it
  discovery.go  the sweep as a probe body: the fan-out, SyncKinds, the kind diff
  kinds.go      one kind's worker: cold list, watch, cookie resume
  state.go      the state types and this leaf's reason vocabulary
```

**One package, split by file.** Discovery and the kind workers differ in their bodies and share
everything else — one connection, one store claim, one identity gate, one suspension, one
teardown. A package boundary between them would cut across the shared half: two leases per cache,
two arming contracts to keep in agreement, a clear that has to hold both, and discovery's own wake
arriving from another package's workers.

One `session` per tracked cache, holding a `kubeconn.Lease`, one `kubestore.Store` claim for the
session's life, a discovery loop, and a worker per kind.

- **The claim is the session's**, not the worker's, so a tracked cache has a file the moment it is
  armed — which is what `Manager.WatchOpen` readers are waiting on.
- **The identity gate is the session's too**, but the two kinds of worker wait differently. A kind
  worker holds its own goroutine, so it blocks on `kubeconn.AwaitConnFor(ServerUID)`, reporting
  each refusal as its own `NoConnection`/`IdentityMismatch` while it waits. Discovery
  runs on the supervisor, where a run holds a supervisor worker and `AwaitConnFor` is documented as
  never to be called — so it commits `NoConnection`, returns `Suspend`, and is woken by the
  connection bridge.
- **Discovery runs on `internal/supervisor`** — three probes over a per-cache subject, `kubeconn`'s
  shape, since both are periodic pulls whose answers are values. `apiVersions` reads `/api`,
  `apiGroups` reads `/apis`, and `resources` fans out over the group list on a data edge from
  `apiGroups`, so a group list that will not load leaves the fan-out `Skip`ped rather than failing
  it. The supervisor owns each one's cadence and backoff ladder and the `Wake` the store bus below
  turns into a prompt re-run; `DiscoveryState` is projected from the snapshot, so the seam stays
  this package's vocabulary rather than the supervisor's. The kind mirrors run on a second supervisor
  over per-kind subjects — a run establishes the stream and commits it as the probe's value,
  rather than being the stream. → [The mirror on the
  supervisor](kubesync-mirror-on-supervisor.md).
- **Discovery is a probe whose collection cannot be watched.** `/api` and `/apis` are plain GETs
  with no resourceVersion and no watch verb, so the sweep is a cold list with no watch phase, and
  it re-lists on the supervisor's cadence where a kind's worker would go live. `SyncKinds` reconciles its answer
  by fingerprint and prune, as a relist does by mark and sweep — and the sweep **skips the write
  when its fingerprint matches the stored one**, since the call is a delete plus an upsert per row,
  six hundred statements for a large catalog, in one transaction against the single writer every
  sync worker's deltas queue behind. **The answer goes to disk and nowhere else** — the sweep
  starts no worker and stops none, because what is mirrored is the records' to say (decision 2);
  it publishes news and the mirror pass does the rest.
- **What a sweep keeps is narrower than what it reads.** Four filters, none optional — a kind that
  gets through is one a worker can actually mirror:
  - **The preferred version only**, one per group. Every served version mirrors the same objects
    again: two `kind_catalog` rows, two workers, two watches, two copies of every row over one
    storage.
  - **`list` and `watch` in its verbs.** A create-only kind — `tokenreviews`,
    `subjectaccessreviews`, `bindings` — is a worker that can only fail.
  - **No `/` in the plural.** `pods/log` and `deployments/scale` are subresources with no
    collection behind them.
  - **Not the `events.k8s.io` spelling of Event.** One store backs both, and `v1`/`events` is the
    one that is synced.
- **`IsCRD` comes from a CRD list, not from discovery**, which describes a custom resource exactly
  as it describes a built-in. Matched by (group, plural) with no version, since one definition
  serves several and a kind found at any of them is the same custom resource. **Best-effort, and
  outside the verdict**: listing CRDs is a cluster-scoped read RBAC commonly denies, and failing a
  sweep over it would take discovery away from users it otherwise serves — a refusal leaves every
  kind reading as built-in.
- **A group that cannot be read degrades discovery, not the kinds already found.** Their workers
  watch independently of the sweep and report their own verdicts, so a broken aggregated API shows
  up twice and correctly: once as an unreachable group-version, once as those kinds failing to
  sync. What it does block is the prune — `SyncKinds` takes one prune flag for the whole answer,
  so a cluster with a permanently broken group never drops a departed kind. Scoping the prune to
  the group-versions a sweep actually read is the fix, and it is a change to the store's API
  rather than to this seam.
- **The sweep's wake comes off the store bus**, subscribed on the CRD and APIService object keys.
  Those two kinds are what change a catalog and the cache already mirrors both, so a private watch
  here would be a second watch on the same collections over the same connection. The loop it forms
  — discovery starts the workers whose writes wake discovery — bottoms out on the cadence, which
  is also the cold start. It can only ever be a wake: an api server upgrade changes the built-in
  kinds with no CRD or APIService write at all.
- **A worker** is a standing push stream, not a periodic pass: a paginated cold LIST through
  `BeginReplace`/`WritePage`/`Commit` — behind a fleet-wide gate, since arming a cache arms
  hundreds of kinds at once — then a WATCH from the cookie through `ApplyChange`. Events age out
  through `PruneEvents`.
- **A commit publishes** an observation and signals only when the reason moved, so counts and
  timestamps ticking never requeue a record.
- **A resume holds its reason.** A worker restarting off its cookie stays at `Watching` while it
  reconnects, and publishes only if the resume fails or outlasts the staleness threshold. Without
  this, `RestartAll` walks every kind through `Watching`→`Syncing`→`Watching` — three reasons, so
  run aggregation cannot collapse them — and a resume poke on a 300-kind cache becomes six hundred
  reconciles and six hundred event runs against a single-writer store, every time a laptop opens.
  A cold start is different and still reports `Syncing`: there the kind genuinely has no data.

## Open: stopping workers across a clear

`Caches().Clear` deletes a cache's file and reopens an empty one under whoever holds it, and
`CachedKinds().Clear` deletes one kind's rows and its resume cookie. Both are two steps, and a
worker running across them lands wrong in two distinct ways: one mid-relist keeps writing pages
into the file the manager has already unlinked, and one that resumes from its cookie afterwards
applies deltas to an empty database with no cold list behind them, leaving the cache permanently
short of what it held.

So a clear needs the affected workers stopped across the swap and unable to restart until it is
done, and stopping them is not something a caller can arrange from outside kubesync. What that
looks like on the seam — a scoped hold, a callback the clear runs inside, or moving the clear
itself behind kubesync — is undecided, and so is how the health fold avoids reporting a clear in
progress as a cache that stopped syncing.

## Rules

- **kubesync never names a record.** It knows a store's id, a kube-context, a server UID and GVRs
  — never that a `ClusterCache` record is named after the store it writes into. `cacheID` is
  kubestore's file key; that a record shares the number is the caller's business, and turning one
  into a name is the trigger's. A record type reaching this package is an import cycle, which is
  the enforcement.
- **Arming is policy, never interest.** A worker starts because a record's pass armed it, never
  because something read it.
- **Nothing syncs into a cache whose connection does not vouch for its `ServerUID`.**
- **A verdict is a gauge; a transition is an event.** Neither is a stored condition — a condition
  would serve a dead process's answer until the passes caught up, and the gauge is the live one.
- **News is not data.** The key is the whole message; the reader re-reads.
- **A published slice is immutable.** Whatever a getter hands out is replaced whole on the next
  answer, never written into, so no read has to copy under the lock.
- **No answer is not an empty answer.** A getter reporting false means nothing has been observed
  yet — a fresh process, an unreachable cluster — and a caller that folds it into "serves no kinds"
  deletes a record set that was only waiting.
- **The cache gates, the record arms.** A kind runs when both say so, and a registered kind
  survives its cache being paused, cleared, or not yet tracked.
- **A resume is not news.** Only a reason that settles somewhere new is published; a watch
  re-established off its cookie changed nothing a reader can act on. Discovery has one more
  publication: a `SyncKinds` that committed. Two sweeps can both settle on `Discovered` with a CRD
  appearing between them, so a reason-only feed would leave the new kind unmirrored until something
  unrelated moved. The fingerprint-skip is what keeps this honest — an unchanged catalog writes
  nothing, so it is news exactly when the set changed.
- **Forgetting is synchronous.** `ForgetDiscovery` returns only when no worker can still write
  through that cache, and `ForgetKind` only when that kind's cannot.

## Build order

Each step is one red/green cycle and one commit.

1. **The skeleton**: `TrackDiscovery`/`ForgetDiscovery`, `TrackKind`/`ForgetKind` and the
   relationship between them, `RestartAll`, the lease and store claims, the identity gate, the two
   reads and the two news feeds — with the discovery and sync bodies as seams a test substitutes.
   Nothing above it changes yet.
2. **Discovery**: the sweep, `SyncKinds`, the kind diff, the cache-level reasons.
3. **The kind worker**: cold list, watch, cookie resume, the per-kind reasons and freshness stamps.
4. **The triggers and the two passes**: the cache arming discovery and mirroring the record set,
   each record arming its own kind, and the two event timelines.
5. **The gauges and the clears**: `WatchHealth` onto the getters, `WatchSyncStatus`, and whatever
   the open item above resolves to.

Steps 1–3 are `kubesync` alone and land before anything consumes them. Between 3 and 4, [the
mirror moves onto the supervisor](kubesync-mirror-on-supervisor.md), which changes step 3's
internals and nothing on the seam.

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
loses the "nothing fills a cache" notice, and the root `CLAUDE.md` loses its copy. Two decisions
are owed an ADR — the cache-scoped seam with arming as policy, and records as timeline anchors
rather than status mirrors. Delete this spec when the last step lands.
