# TODO

Pending work across the three parts of the app. Grouped by area; detailed items keep their acceptance notes inline.

> **The specs are the plan.** Work with a settled shape lives in [`docs/specs/`](specs/) and is not repeated here. This file holds what has no spec yet: watch items, simplifications, and work whose shape is still a question.

## Sidecar — cluster service

- **Map `clustersvc.ErrNotFound` to `errors.ErrRecordNotFound` in the resolvers.** The boundary reports a missing record with its own sentinel (`sidecar/internal/clustersvc/shared.go`) and `graph/errors` declares the wire error with its `KSTACK_RECORD_NOT_FOUND` code, but nothing joins them — no resolver references `graph/errors` at all yet. So `clusterEnabledSet` against a deleted id reaches the webview as the raw internal string rather than a coded error a client can branch on. The mutation resolvers are all implemented now, so nothing is waiting on: the pass just has to happen, and it decides where the mapping lives (an error presenter, or per resolver). `clustersvc.ErrDeclaredBySource` needs the same treatment and a code of its own: `clusterDelete` refuses a record its source still declares, and a client cannot tell that refusal from a store failure without one.

- **The cluster controller re-derives its source edge every pass.** `clusterController.Reconcile` (`sidecar/internal/clustersvc/clusters.go`) declares the dependency onto the kubeconfig anchor by looking the anchor up by name and calling `AddDependency` on every reconcile: a `GetByName` row read plus an `Edges().Add` transaction (a `SELECT` join and an `INSERT ... ON CONFLICT DO NOTHING`), whether or not the edge already exists. **Not a performance problem** — both are indexed round-trips, the expensive half (the requeue signal) is already gated on `ReconcileOwedStamped` so it fires once per edge ever created, and passes are paced by `clusterProbeInterval` (5m) plus events. Listed for the **simplification**, and because the cost is worth knowing before anyone puts this pass on a hot path.
  - **Fix:** own the edge instead of re-deriving it. `ensureKubeconfigClusters` runs inside the source controller's reconcile, so the anchor's ID is in hand — create the Cluster with `beehive.WithOwner(...)` and let the pass read `client.GetOwner(ctx)`, exactly as `cacheController` already does. That deletes the `sourceClient` read, the `ErrNotFound` startup-requeue branch, and the `Spec.Source.Kubeconfig != nil` special case, and a future non-kubeconfig source inherits the wake instead of needing its own branch. Store round-trips stay at two (`GetOwner` is also a read), so this buys clarity, not speed.
  - **Check before doing it:** stored Clusters predate the edge, so `GetOwner` reports no owner for them and they would never get the dependency — a non-issue under the pre-release policy (edit `0001_init.sql`, delete the dev `app.db`), but it is why the change is not purely mechanical. Also confirm `WithOwner` does not change Cluster lifecycle: owner edges cascade on delete, and nothing deletes an anchor today, so the risk is latent rather than live.
  - **Fix the stale comment either way:** the block's comment claims "every later pass is free", which is only true of the *wake*. The edge upsert still costs a transaction per pass.

- **`Cluster.caches` is an N+1 now that `ListByCluster` answers.** A query selecting it calls the resolver once per cluster, and each call is a join query plus a batched owner-edge read; the store is single-connection, so they serialize. Harmless at present row counts (tens), and the fix is a gqlgen dataloader rather than anything local — a per-resolver `List`-and-bucket would be worse, since the resolver still runs per cluster. **Trigger:** the first surface that selects `caches` over the whole fleet, or the first report of a slow `clusters { caches }`.

- **Nothing exposes the four non-connection probes, so "Connected but not Identified" has no detail.**
  `kubeconn.State` carries a full `Observation` per probe — value, `LastSeen`, `LastAttempt`
  (verdict/reason/message), `Failures`/`FailingSince`, `NextAttempt` — and `foldState`
  (`sidecar/internal/clustersvc/clusters.go`) deliberately copies only the **values** into
  `ClusterStatus`, since a status that moved every pass would re-emit the record to every watcher on
  every cycle. So a cluster whose credentials cannot read `kube-system` surfaces as one condition
  reason (`ReasonUIDUnreadable`) and nothing else: no failure count, no "next attempt in 4m", no
  distinction between the serverUID probe failing and the principal probe failing.
  **Shape to build:** a per-probe row — name, `lastAttempt {reason, message, at}`, failures /
  failing-since, next attempt, in flight — on its own subscription (a gauge, like the schedule: it
  moves after the record settles, so it must not be a field on `Cluster`; see the gauge bullet in
  `sidecar/CLAUDE.md`). **Note what it subsumes:** `Schedule.probing` becomes a field on the
  connection's row, read off the same snapshot. **Weigh:** five bare `nextAttemptAt`s would be no
  more useful than the one `clusterScheduleWatch` already serves — the reason/failure detail is
  the whole point, so build the row or build nothing.

- **`classify` has no `*apierrors.StatusError` branch.** Every probe reads a raw path today, so a
  status code is the whole evidence and `statusReason` covers it. **Trigger:** the first probe that
  goes through `Connection.Dynamic`, which returns the API's own reason — classifying that on the
  status code alone discards what the typed half already knows (`state.go` states the rule).
  `kubesync` already uses `Dynamic`, but no probe does, so nothing has reached it yet.

- **`supervisor.Supervisor`'s `Wake`/`WakeAll` pair reads as one axis and is two.** `Wake(subjectName string, names ...string)` takes named probes on **one** subject; `WakeAll(names ...string)` takes named probes across **every** subject. The variadic means the same thing in both, and `All` varies the argument that is not there — so `WakeAll` reads as "wake every probe" when it means "wake these probes everywhere". Both call sites are correct today: `watchKubeconfig` wants `WakeAll(nameConnection)` (one probe, whole fleet) and `RetryAndWait` wants `Wake(contextName, probeNames[:]...)` (one context, every probe) — they are exact transposes, which is what makes the pair easy to reach for backwards. **Fix:** rename `WakeAll` to name its axis (`WakeEverySubject`, or `WakeSubjects`), two call sites plus `engine_test.go` and the `sidecar/CLAUDE.md` wiring line. **Weigh:** the engine is a general leaf and `WakeAll` is the shorter, more conventional spelling; the case for renaming rests on the pair being read together, which is exactly when the ambiguity bites.

- **`supervisor.Supervisor`'s run queue has no debounce.** `runQ` and `passQ` are `internal/workqueue`
  queues (`sidecar/internal/supervisor/supervisor.go`). **Deduping is not debouncing** — a key waits once,
  but only while it is waiting, and one added while a worker holds it is queued afresh on `Done`,
  so asks spread across a run are a run apiece. For the connection probe each one re-reads the
  kubeconfig and the CA files behind it, then dials `/api`.
  - **Four producers reach one context's key today**: `Acquire` → `engine.Add` (once, per new
    context), `watchKubeconfig` → `WakeAll(nameConnection)` (every claimed context, per kubeconfig
    change), `RetryAndWait` → `Wake` (all five probes, on demand), and the engine itself — the data edge
    on a committed value, and the subject timer arming the next due pass.
  - **Not a problem yet**, and one throttle already exists: `WithWorkers` caps runs in flight
    fleet-wide, which is what holds back the first pass over a large kubeconfig so every cluster's
    credential helper does not run in the same second. It bounds concurrency, not the number of
    runs. The producers are also quiet — a claim asks once per new context, and `kubeconfig.Service`
    polls on a ticker and publishes only when the whole loaded config differs (`reflect.DeepEqual`),
    so a hand-edited file produces one signal rather than one per write.
  - **The trigger has partly fired.** `RetryAndWait` is the third producer and the first
    *user-driven* one: a client that retries on a timer, or a user clicking through an outage, asks
    for one context repeatedly, and dedup merges those asks only while the key is waiting. Bounded
    by more than a person's click rate now — the mutation is held open for the probe's round trip
    and the button is disabled for all of it — so this is a watch item rather than work, until a
    producer that can ask in a loop makes it real.
  - **Home:** `internal/workqueue`, as its own feature. The old pairing with `AddAfter` is gone:
    the engine schedules delayed work with a per-subject `time.AfterFunc` over a schedule derived
    in `pass`, so nothing wants a delayed queue add any more.
    [amorey/gobus#17](https://github.com/amorey/gobus/issues/17) proposes the same queue upstream;
    this package is the worked shape to draw from if it lands.
  - **Weigh the alternative first:** a floor belongs to whoever knows what a run costs, and that is
    the engine (it owns the cadence, the backoff, and `WithWorkers`) rather than a generic queue. A
    per-probe minimum interval — "this probe runs at most once every N" — would cover the same
    burst without a second timing mechanism in the queue.

- **Four SQLite pragmas nobody has measured.** The table shapes and the open contract are
  settled — every table is `STRICT`, the small keyed tables are `WITHOUT ROWID`, the three
  rowid tables each say in `0001_init.sql` why they are one, and `sqlitemigrate.OpenPool`
  is the single home for the writer/reader DSNs. What has never been looked at is
  `PRAGMA optimize` on close (it is what keeps the query planner's stats honest as a cache
  fills), WAL checkpoint/truncate behaviour under a long-lived writer, `page_size` against
  the compressed-body row width, and `mmap_size` for the cached-data read path. **The
  instrument for the WAL question already exists**: `Stats` measures the three files apart
  and the panel's size cell shows the split on hover, so a WAL that dwarfs the sqlite file
  is visible without adding anything.
  - **Measure first, and against a real fleet**: a cluster with CRDs, an event-storming
    namespace, and a cache that has been through a relist or two. The numbers that matter are
    file size versus rows held, and whether a vacuum sweep is visible as sync latency. None of
    these are guesses to apply blind.

- **Paged object watches.** Nothing pages anywhere: `clusterCachedDataObjectsWatch` snapshots a kind's whole set and ships every `rawJSON` body over the IPC bridge, then re-reads the whole set per debounced burst. On a large kind that is the dominant cost of the dashboard — bigger than anything [the read split](adr/2026-08-29-object-read-split.md) addresses, which only shrinks the sidecar's half.
  - **The mechanism already handles it; the read does not.** Bound `Store.Objects` by a keyset range — `AND (namespace, name) > (?, ?) ORDER BY namespace, name LIMIT ?` — and `runCachedDataWatch` diffs a window instead of a collection, with no change to the loop. **Re-deriving is what makes the hard cases free:** an insert above the window, a rename that moves a row out of it, a delete that pulls the next row in are all just "what is in the range now vs. what was", never a shift to reason about. Two things are already in place: `objects_kind_ns_name(api_version, kind, namespace, name)` is exactly the keyset index for that order, and `kind_counts` gives the total for a scrollbar in O(1) with no scan.
  - **Settle the sort/filter contract first — it is the whole decision.** Paging trades *sort and filter by anything, client-side* for *sort and filter by what the store indexes*. `objects` carries `namespace`, `name`, `created_at`, `status_summary`, `ready_count`/`total_count`, `restart_count` and `host` as real columns, and `labels` is joinable for selector filters, so the common orders are servable. What is not: the kind-specific columns `src/components/widgets/object-columns.tsx` derives from `rawJSON` client-side — sorting a paged Pods table by Restarts asks for an order the store cannot produce. Either accept that those columns do not sort, promote more of them into columns at write time (`projectObject` already extracts several), or do not page. A product call, not a storage one.
  - **The protocol wrinkle:** subscription variables are fixed for the life of a subscription, so moving the window means resubscribing — a debounced resubscribe per scroll settle, re-snapshotting a window's worth of rows. Cheap, but it makes the window a subscription variable (or a search param) rather than client state, and every move is a round trip.
  - **Not what the change log is for.** A log tail names the uid that changed, which is not a position in a sort order: inserting one object above the window shifts a row out of the bottom, and the entry mentions neither of the two objects whose page membership moved. Kubernetes offers `limit`/`continue` on list and nothing on watch for the same reason.

- **Revisit an `object_writes` change log in `kubestore`.** The cached-data watches learn what changed by re-reading a collection and diffing it by key (`sidecar/internal/clustersvc/cacheddatawatch.go`), which is O(collection) per debounced burst however cheap each row is made — the [read split](adr/2026-08-29-object-read-split.md) took the bodies out of that scan but not the scan. An append-only log, one row per committed write carrying a local monotonic cursor, turns the tail into O(changes), the way beehive's own `object_writes` does for its store. **Paging shrinks the case rather than making it:** once the read is bounded to a window, O(collection) per burst stops being a number anyone measures, so do the item above first and re-ask.
  - **What it buys that the diff cannot**, and it is only these three: O(changes) rather than O(uids scanned); *every* transition rather than the coalesced net, since a debounce window collapses two writes into one frame; and a cursor a reader can resume from instead of re-snapshotting.
  - **Trigger:** a surface that wants a per-object change history — the second one is a capability, not a speed-up — or a frontend that resumes a watch across a reconnect. Neither exists yet: `useWatchSubscription` discards the previous generation's accumulator by design, so a persisted cursor would have no consumer.
  - **The two things that make it more than a table.** A relist rewrites every row (`kubestore/objects.go`, stated in `recordStatusTransition`'s comment), so an unconditional append turns each cold list into one entry *and* one `Modified` frame per object — a regression against today's diff, which sends nothing for an unchanged row. The append has to be conditional on the incoming `resource_version` differing from the stored one and ordered before the upsert in the same transaction; `recordStatusTransition` already has that shape. And there is no local sequence to key on: `objects.resource_version` is the *Kubernetes* one — TEXT, from the cluster's etcd, neither ours nor ordered — so the log needs its own `INTEGER PRIMARY KEY` counter, scoped to one cache file, which a `Clear` resets to zero.
  - **Scope, so nobody starts it by accident:** the table, a covering index and an age index, a horizon table and a retention sweep, a final row image on delete entries (a `Deleted` frame reports a whole object, and the row is gone by then), and a fall-back-to-snapshot path for a reader that has fallen below the horizon. Roughly what beehive carries for the same job. It lands in the file the janitor is already sweeping, so sequence the two.
  - **Not the events spec.** `clusterCachedDataEventsWatch` — k8s Events mirrored into the cache file — would ride the same log. Beehive's control-plane event timeline, which `clustersvc/events.go` serves, already has its own and is unaffected either way.

- **One measurement per cache for the stats gauge, not one per subscriber.** `cachesAPI.WatchStats` builds its whole loop inside `NewStream`'s pump (`clustersvc/caches.go:347`, `stream.go:180`), so every subscriber gets its own: three windows on one cache means three 5-second tickers, three file measurements, and three sets of row-count queries against the same file. The counts are the expensive half — the file measurement is three `os.Stat` calls, the counts are SQL. Nothing is wrong with the answers; the work is just done N times. **Shape:** one measurement stream per cache, multicast to its subscribers, the way the delta watches already fan out — the pump moves off the subscription and onto the cache, and a subscriber joins the running one and gets the current value on arrival (the gauge is current-on-subscribe, so a joiner must not wait for the next tick). `WatchHealth` has the same per-subscriber shape over the records, so whatever carries the multicast should be able to serve both. **Not blocked on anything**, and independent of the cache size ceiling: the janitor's own measurement is a different caller with a different lifetime, and is not what this de-dupes.

- **OAuth access-token refresh — background/proactive half.** On-demand refresh is done (`sidecar/internal/auth/grant.go` refreshes a lazily-expired token using the stored refresh token). What remains: a proactive/background refresh before expiry rather than only refreshing when a consumer hits an already-expired token.
- **SSO failure didn't retry.** The async login tail (wait-for-redirect → exchange → verify → persist) is fire-and-forget; a tail failure is only logged and leaves the session signed-out (a known v1 limitation), with no retry. The user must manually re-initiate login.
- **Check RBAC permissions?** The `ClusterPermissions`/`ResourceRule`/`NonResourceRule` types and schema exist, but the `Permissions` resolver is a stub that returns `not implemented: permissions`. Implement it via a `SelfSubjectRulesReview`. Distinct from the `SelfSubjectReview` *authentication* probe behind `ClusterPrincipal.username`, which is implemented.

## Sidecar (Go)

- **Hoist `Condition`/`Event`/`Schedule`/`ObjectRef` when a second consumer appears.** All four are kind-agnostic on the wire (unprefixed, per the schema's naming rule) but live in `internal/clustersvc` (`shared.go`) because the cluster surface is their only consumer. **Trigger:** the first non-cluster kind or subsystem that needs conditions, events, schedules, or owner refs — at that point move all four into a shared leaf package (e.g. `internal/apimeta`), leaving `clustersvc` its `ConditionType` constants (`Connected`/`Identified`/`Synced`). `ObjectRef` takes `toOwnerRef` with it. Hoisting earlier would be a one-importer abstraction.

- **Hoist the doubling-backoff ladder into a shared leaf when a second consumer appears.** Only `prefsync`'s `backoffDelay` (`internal/cloud/prefsync/engine.go` — `baseBackoff << attempt`, clamped to `maxBackoff`, then jittered, with a `withBackoff(base, max)` test seam) computes one by hand: everything inside the control plane rides beehive's own per-object ladder instead. **Trigger:** the next thing that cannot ride beehive's — anything outside the control plane, which is what `prefsync` is. At that point extract base/max/jitter and the `Reset`-on-success discipline into a leaf (e.g. `internal/backoff`) with the same parameterized-cadence seam the testing conventions require. Note the two readings a shared type has to keep expressible: `prefsync` counts attempts across reconnects, where a pass-oriented ladder re-levels on any clean pass.

## Frontend (webview)

- **Built-in kinds with no columns.** The per-kind registry (`src/components/widgets/object-columns.tsx`)
  carries hand-written accessors for **Pod and Deployment only**; every other built-in falls back to
  the universal Namespace/Name/Age. CRDs are handled — they carry their own `printerColumns` — so
  what remains is the obvious built-ins: StatefulSet, ReplicaSet, Node, Service, Job, CronJob, PVC.
  Note **DaemonSet cannot reuse the Deployment/workload accessors** — it has no `spec.replicas`; its
  Ready is `status.numberReady`/`status.desiredNumberScheduled`.

- **Evaluate moving provenance down into `useWatchSubscription` (keyed on urql's operation key).** `useCacheDeltaWatch`'s provenance is a caller-derived string compared against server-echoed fields (`cacheID` + `apiVersion`/`resource` on `ClusterCachedDataObjectWatchFrame`). But one layer down, `useWatchSubscription` already computes `createRequest(query, variables).key` — which changes *exactly* when the watch's variables change, i.e. it is precisely `(cacheID)` for kinds/events and `(cacheID, apiVersion, resource)` for objects, derived automatically for any subscription. And that file already implements this same tag-fold-mask pattern for the sibling dimension (`generation`, for reconnects). **Fix:** make `Generational<Result>` carry `{ generation, key, result }` and gate both the reducer's fold and the exposed-`data` mask on `key` as well — then `currentProvenance`, `DeltaFrame.provenance`, `joinProvenance`, *and* the `apiVersion`/`resource` schema echo fields all disappear, and every future keyed watch gets staleness protection for free instead of re-spelling it. **Weigh first:** server-echoed provenance additionally defends against a genuinely *mis-routed* frame (a sidecar/host-bridge multiplexing bug), which a client-side key cannot — nothing currently claims that as a motivation, so if it *is* wanted, say so where the fields are defined, since it's the only thing justifying the schema surface. Also note the current design's failure mode is silent: a mismatch between the two hand-spelled provenance strings drops every frame with no type-level protection.

- **Extract a shared delta-watch test harness; mock `useActiveCluster` instead of its inputs.** `cluster-cached-data-events.test.tsx`, `cluster-cached-data-objects.test.tsx`, and `dashboard-nav.test.tsx` each carry a near-identical block: a `vi.hoisted` `useWatchSubscription` stand-in with `statusState`/`pushReset`, a **re-implementation of the real `applyChange`** inside `vi.mock('@/lib/clusters')`, a `clusterFixture(hasCache, cacheId, serverUid)`, `pushFrame`, and the same `beforeEach`. Two costs: the fake `applyChange` is a second implementation of `src/lib/clusters.tsx`'s real one and can drift from it while tests still pass; and each file still mocks the **pre-refactor** seams (`clusters` + `active-kube-context`) when `useActiveCluster()` is now exactly the seam they want — mocking it deletes `clusterFixture` and the `active-kube-context` mock from all three. The provenance straggler/swap cases are now covered once in `use-cache-delta-watch.test.tsx`; the copies in the events and objects suites can go.

- **A component is placed where it is to dodge a transport gap.** urql shares one operation between identical subscribers and its replay is query-only, so a second subscriber to a live subscription sees nothing until the next frame. The panel works around it by structure — hoisting `useCacheContents` to `ClusterRow` — enforced only by a long comment. **Cost:** component placement is load-bearing and silently re-breakable by any future consumer. **Fix:** put replay in the layer that owns the connection — have `subscribe-exchange.ts`/`useWatchSubscription` keep the latest value per `operation.key` and hand it to a late subscriber on attach.

- **`clusters.tsx`'s join memo rebuilds every cluster on every health frame.** The memo depends on `healthMap`, so a sync-health frame (re-emitted per changed cache on a periodic tick) produces new identities for *all* clusters and *all* joined caches, invalidating every downstream memo keyed on them; the cache lookup inside is a linear `caches.find` per cluster on top. **Fix:** build a `Map<clusterID, cache>` once, and split the join — memoize cluster+cache on `[clusterMap, cacheMap]` only, then attach `health` per row in a child subscribing to just its cache's entry, so one moved cache re-renders one row.

- **`useCachedKinds` folds ~150 records into a map and array to yield two lookups.** `SyncDetail` uses the list twice and never as a list: `timelineSyncFor` picks one record's `{ id, resource }`, and `idOf` does a linear `find` per failing kind to map a GVR back to its record id. **Fix:** return a `Map` keyed by `gvrKey` alongside the timeline record, so both lookups are O(1) and neither needs the array. Collapsing the hook to just the timeline record does not work — it would leave `idOf` with nothing to search.

- **`cluster-sync-panel.tsx` is ~1300 lines holding five separable units.** Thirteen `graphql()` documents and seven subscription hooks, a status/tone/formatting vocabulary, and three independent panels (`ConnectionDetail`, `SyncDetail`, `ClusterRow`) plus the dialog shell. `overallTone` is exported only because the test file has nothing narrower to import. **Fix:** a `cluster-sync-panel/` directory — `subscriptions.ts`, `status.ts`, `connection-detail.tsx`, `sync-detail.tsx`, `cluster-row.tsx`, `index.tsx` — leaving the panel file as a ~110-line shell. Pure churn, so worth doing when the file is next touched substantively rather than on its own.

- **Investigate consolidating the small `src/lib` modules into a shared `util.ts`.** `src/lib` has grown a tail of very small files — `gvr.ts` (27 lines), `platform.ts` (31), `dashboard-nav.tsx` (32), `active-cluster.tsx` (42), `error-bus.ts` (50), `connection-status.tsx` (54), `window-maximized.ts` (58), `active-kube-context.tsx` (60) — and the per-file navigation/import overhead is starting to outweigh the separation. **The distinction to make first, before moving anything:** most of these are *not* generic utilities, they're cohesive domain modules that happen to be short — `active-cluster.tsx`, `dashboard-nav.tsx`, `active-kube-context.tsx`, `connection-status.tsx` are React hooks/providers with a clear single responsibility and their own consumers, and folding those into a grab-bag would trade a real boundary for a line count. The genuine candidates are the **pure, dependency-free helpers**: `gvr.ts` (a type + one key function) and `platform.ts` (sync `isMacOS()`/`isLinux()` UA checks); `window-maximized.ts` and `error-bus.ts` are borderline. **Tradeoffs to weigh:** a shared `util.ts` is a magnet — it accretes unrelated helpers, blurs ownership, and (if it ends up importing React/urql/domain modules) can create import cycles that the current leaf files structurally can't; against that, a dozen 30-line files means more files to open to follow one flow. **Output:** either a small `util.ts` holding only the pure leaf helpers (with a rule for what may go in), or a decision to leave the split as-is and treat file count as acceptable — recorded either way so this doesn't get re-litigated.

- **Investigate urql caching — are we using it correctly?** We've never audited how the urql client is configured (`src/lib/graphql/client.ts`) w.r.t. caching. Confirm which cache is in play (the default **document cache** vs. `@urql/exchange-graphcache`) and whether it fits our access patterns: queries/mutations over Tauri IPC (`invoke-fetch.ts`), and the many delta-watch subscriptions that maintain their own reduced state via `useWatchSubscription`. Questions to answer: does the document cache help or just add staleness for our mostly-subscription data; are mutations correctly invalidating/refetching the right queries (e.g. cluster enable/sync toggles vs. `clustersWatch`); is `requestPolicy` set intentionally; and would normalized caching (graphcache) actually buy us anything given watches already own the live state. Output: a short note on the current behavior + whether to change the exchange pipeline or leave it.

- **Startup URLs don't reference the active kube context.** A fresh window lands on a bare `/chat` (`index.tsx` redirects `/` → `DEFAULT_ROUTE` with no search); `useActiveKubeContext` resolves the context by *falling back* to `kubeConfig.currentContext` but only *writes* `?kubeContext=` on an explicit pick. Consequence: the landing URL isn't self-describing or deep-linkable until the user interacts, and two windows on different default-resolved contexts look identical in the URL. Fix: seed `kubeContext` from the resolved current-context at boot (e.g. `index.tsx` redirect or an `_app` `beforeLoad`). Catch: at `beforeLoad` the clusters stream may not have delivered its first frame, so current-context isn't known synchronously — either accept a sometimes-omitted param or resolve+write once after the first frame lands.

## Security

The current picture — boundaries, and which protections a test actually pins — is
[`security-model.md`](security-model.md); the findings behind these items are
[`security/2026-09-02-threat-model.md`](security/2026-09-02-threat-model.md).

**Each item has a spec.** They live in [`docs/specs/`](specs/), numbered in build order, and are not
repeated here:

8. [Updates say what they actually are](specs/8-updates-say-what-they-are.md)
10. [A kubeconfig exec plugin waits for approval](specs/10-approve-exec-credential-plugins.md)
11. [The host sends only the operations the app ships](specs/11-allowlist-graphql-operations.md)
14. [A cache over its ceiling stops growing](specs/14-stop-a-cache-over-its-size-ceiling.md)
15. [A cache stops outliving the user's interest in its cluster](specs/15-cache-retention.md)

An item we decide **not** to do becomes an ADR rather than a deletion, so an accepted risk stays
distinguishable from an unnoticed one. **Decided against:** aging out cached events by their own
TTL — 13 and 14 bound the whole file instead, and between relists events still accumulate without
a bound of their own. → [bound the cache by total size](adr/2026-09-03-bound-the-cache-by-total-size.md)

**Without a spec yet:** the threat model's H-3. `src-tauri/entitlements.plist` sets
`com.apple.security.cs.disable-library-validation` so the hardened runtime will exec the sidecar,
but `release.yml` now signs the sidecar inside the bundle under the same Team ID, which is the
condition that entitlement was working around. Try a release build without it; if the sidecar
still launches, drop the entitlement, since it is also what would let an injected dylib load.

## Testing

- **Keep the no-wall-clock rule.** Both `CLAUDE.md` files state it, and the tree is currently clean: the frontend suites use `vi.useFakeTimers()` + `advanceTimersByTimeAsync`, `waitFor`, or the `flush()` helper with no `setTimeout` waits; Go's three `time.Sleep` calls are all the permitted kind and each says so — a widened truncate window in `kubeconfig_test.go`, a writer racing a gauge in `caches_test.go`, and `kubesync`'s deliberate exit latency; and `src-tauri/.../sidecar/ipc.rs`'s retry test — `#[tokio::test(start_paused = true)]`, letting tokio auto-advance virtual time between parked timers — is the shape to match. **What to watch for:** not `time.Sleep` but *thin real-timer margins* — tests that never sleep yet still fail on a loaded machine because they race short real durations. The fix shape is an injectable clock/timer seam so the test advances virtual time. The ~30 `time.After(...)` uses in sidecar tests are almost all *deadlines* guarding a channel receive, which the rule explicitly permits; keep those separate from any load-bearing wait. A `-count=20` soak on a loaded machine is the cheapest way to find regressions.
