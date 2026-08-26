# TODO

Pending work across the three parts of the app. Grouped by area; detailed items keep their acceptance notes inline.

> **The sidecar's cluster backend was torn out** (see `sidecar/CLAUDE.md`). Every item that described the removed cache system — the sync path, the store schema, the controllers, and both `/simplify` deferral lists — was deleted with it rather than kept as stale detail, on the same rule as the culled cache ADRs: git history holds them, and the rebuild records its own decisions as they land. Only work that still has code behind it is listed below.

## Sidecar — cluster rebuild

- **Map `clustersvc.ErrNotFound` to `errors.ErrRecordNotFound` in the resolvers.** The boundary reports a missing record with its own sentinel (`sidecar/internal/clustersvc/shared.go`) and `graph/errors` declares the wire error with its `KSTACK_RECORD_NOT_FOUND` code, but nothing joins them — no resolver references `graph/errors` at all yet. So `clusterEnabledSet` against a deleted id reaches the webview as the raw internal string rather than a coded error a client can branch on. Wire it as the mutation resolvers are filled in, and the same pass decides where the mapping lives (an error presenter, or per resolver). `clustersvc.ErrDeclaredBySource` needs the same treatment and a code of its own: `clusterDelete` refuses a record its source still declares, and a client cannot tell that refusal from a store failure without one.

- **The cluster controller re-derives its source edge every pass.** `clusterController.Reconcile` (`sidecar/internal/clustersvc/clusters.go`) declares the dependency onto the kubeconfig anchor by looking the anchor up by name and calling `AddDependency` on every reconcile: a `GetByName` row read plus an `Edges().Add` transaction (a `SELECT` join and an `INSERT ... ON CONFLICT DO NOTHING`), whether or not the edge already exists. **Not a performance problem** — both are indexed round-trips, the expensive half (the requeue signal) is already gated on `ReconcileOwedStamped` so it fires once per edge ever created, and passes are paced by `clusterProbeInterval` (5m) plus events. Listed for the **simplification**, and because the cost is worth knowing before anyone puts this pass on a hot path.
  - **Fix:** own the edge instead of re-deriving it. `ensureKubeconfigClusters` runs inside the source controller's reconcile, so the anchor's ID is in hand — create the Cluster with `beehive.WithOwner(...)` and let the pass read `client.GetOwner(ctx)`, exactly as `cacheController` already does. That deletes the `sourceClient` read, the `ErrNotFound` startup-requeue branch, and the `Spec.Source.Kubeconfig != nil` special case, and a future non-kubeconfig source inherits the wake instead of needing its own branch. Store round-trips stay at two (`GetOwner` is also a read), so this buys clarity, not speed.
  - **Check before doing it:** stored Clusters predate the edge, so `GetOwner` reports no owner for them and they would never get the dependency — a non-issue under the pre-release policy (edit `0001_init.sql`, delete the dev `app.db`), but it is why the change is not purely mechanical. Also confirm `WithOwner` does not change Cluster lifecycle: owner edges cascade on delete, and nothing deletes an anchor today, so the risk is latent rather than live.
  - **Fix the stale comment either way:** the block's comment claims "every later pass is free", which is only true of the *wake*. The edge upsert still costs a transaction per pass.

- **`ClusterCache` serves no record metadata, so it cannot carry a tombstone.** `Cluster` exposes `generation`/`createdAt`/`deletionRequestedAt`; `ClusterCache` exposes none of them, so `Caches().Get` on a cache mid-delete answers a record with nowhere to put the mark and no consumer can tell it is going. The boundary's "a read reports the store as it is, and never filters" rule therefore holds for `Cluster` and is unrepresentable for `ClusterCache`. The schema shape predates the rebuild; serving the family is what turned it from dormant into a real gap. **Fix:** falls out of the `recordMeta` extraction below — do it there rather than adding three fields by hand.

- **`Cluster.caches` is an N+1 now that `ListByCluster` answers.** A query selecting it calls the resolver once per cluster, and each call is a join query plus a batched owner-edge read; the store is single-connection, so they serialize. Harmless at present row counts (tens), and the fix is a gqlgen dataloader rather than anything local — a per-resolver `List`-and-bucket would be worse, since the resolver still runs per cluster. **Trigger:** the first surface that selects `caches` over the whole fleet, or the first report of a slow `clusters { caches }`.

- **Share the object→record metadata mapping.** `toCluster` copies five fields straight off the beehive object (`ID`, `Generation`, `CreatedAt`, `DeletionRequestedAt`, `Conditions`); `toClusterCache` and `toClusterCachedCatalog` copy two of them and carry an `Owner` instead. **Fix:** a `recordMeta` struct embedded in every record, filled by one `toRecordMeta[S, T any](obj *beehive.Object[S, T]) recordMeta`, so each `toX` shrinks to metadata + spec + status and a new beehive metadata field is one edit rather than five. The owner half is already shared (`toOwnerRef` in `shared.go`), so what remains is the five metadata fields. **This is not the refactor its sibling was** — the delta-watch pump extracted with no wire change, but the child kinds serve *none* of the three metadata fields today (see the tombstone gap above), so embedding a shared `recordMeta` either adds them to the schema for two kinds or embeds a struct whose fields are deliberately unserved. Decide which before starting.
  - **Verify first:** that gqlgen autobinds *promoted* fields from an embedded struct. If it does not, every record needs explicit `fields:` mappings in `gqlgen.yml` and the trade is not worth making.
  - **What it will not remove:** the id conversion, because the `ObjectID` scalar binds `clustersvc.ObjectID` (a defined type) while beehive's is an alias for `int64` — binding `int64` instead would capture every `int64` in the schema; and the status default, because beehive leaves `Status` nil until first written while the schema types it non-null. So `toX` gets small, not deleted.
  - **Upstream alternative, not a substitute:** beehive could factor its own metadata into an embeddable `ObjectMeta` that `Object` embeds (strictly additive there, and every consumer projecting objects into records pays the same copying). Even then the two exceptions above remain, and embedding beehive's metadata wholesale would put `Name`, `ResourceVersion`, and `Finalizers` on the record — `Name` especially, which this package treats as a reconcile key nothing reads back.

- **Discovery's watch has no adaptive cadence, and a refused watcher is invisible.** The sweep is
  watch-prompted now (`sidecar/internal/clustersvc/internal/kubecatalog`), with resourceVersion
  continuation so a quiet server-side timeout costs a reopen rather than a sweep. What is left is
  the tail: stretch the interval while the watch is healthy (needs a per-result interval on the
  engine — a `Succeeded().After(d)` mirroring beehive's `RequeueAfter`); aggregated discovery, one
  request instead of dozens on 1.30+ servers; metadata-only watch payloads. **The one worth
  deciding rather than just doing:** a watcher refused by RBAC still reports `Succeeded()`, since
  the catalog data is fine and only promptness degrades — so a user who may never watch CRDs sees
  a healthy catalog that is quietly up to 10m stale, and nothing says so. Watch health as a field
  on the observation is the shape; whether the fold should surface it at all is the question.

- **The catalog stays resident for as long as a cluster is tracked.** `kubecatalog`'s observable
  holds the whole `Catalog` per subject — every served kind's group-version, kind, resource and
  scope — and the `ClusterCachedResource` rows the fold writes hold the same list again in the
  store. Order of 90 bytes a kind, so tens of KB for a cluster with CRDs and well under a megabyte
  across a large fleet: **listed for the duplication, not the size.** The trigger is a second
  consumer wanting the kinds and reaching for the in-memory copy because it is there, or a fleet
  where it starts to show.
  - **Two things hold it there, and a fix has to answer both.** `pass.Prev()` is the commit guard
    — the sweep commits only on a change, which is the whole of what stops a 10m pass waking the
    fold on a cluster that moved nothing — and `clusterCachedCatalogController.Reconcile` reads the
    standing answer back through `Read(id)` to rewrite the child records. A fingerprint covers the
    first and not the second.
  - **Weigh:** the shape would be the sweep retaining a hash and the fold diffing against its own
    children, which the store already holds. That trades resident memory for a store read per
    pass, and moves the "what changed" diff next to the prune and tombstone rules that already live
    in the fold. It also couples the sweep's correctness to records it must not know about — the
    leaf rule — so the hash has to be enough on its own. Not obviously a win; measure first.

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
  connection's row, so the in-flight publishing gap below stops being a footnote and becomes a
  prerequisite. **Weigh:** five bare `nextAttemptAt`s would be no more useful than the one
  `clusterScheduleWatch` already serves — the reason/failure detail is the whole point, so build
  the row or build nothing.

- **Identity-driven connection retirement is specified and unbuilt.** The connection probe retires
  on a changed credential fingerprint alone; a *server* that changed behind unchanged credentials
  does not retire anything, so a holder would go on using a connection whose cluster is no longer
  the one it derived from. **Fix:** a **known** identity reading differently retires the connection
  — a part filling in from empty is the first identification, not a new server, and treating it as
  one would rebuild every connection once. The connection probe cannot declare a watch edge on the identity probes
  (`resolveLocked` takes only already-registered names, which is what keeps the graph acyclic), so
  promptness comes from `publish` calling `Wake(contextName, nameConnection)` when identity moves —
  a `Wake` is not an edge. **The trigger has fired**: serverUID, serverVersion and principal all
  answer now, so `State.Identity()` is populated and nothing acts on it moving. → [ADR: the
  connection probe dials /api](docs/adr/2026-08-25-connection-probe-dial.md).
  **Spec:** [docs/specs/connection-identity-retirement.md](docs/specs/connection-identity-retirement.md).
  - **Do not record the identity on `connInfo`.** That would have the connection probe read a UID
    off its own snapshot — the stale pairing [ADR: connection-carried
    identity](docs/adr/2026-08-25-connection-carried-identity.md) rejected, where a re-dial landing
    before the UID probe re-runs commits `{conn: B, identity: uid-A}` and makes a transient window
    durable. The identity is already on `Connection`, stamped by the probe that read it, and
    `setServerUID` already **records the conflict** when a second, different uid arrives — which
    makes `ConnFor` refuse every caller. **This item is now only the recovery half:** nothing
    rebuilds a connection whose credentials never changed, so a swapped server stalls
    identity-scoped work over that connection instead of corrupting it. Wire `publish` to
    `Wake(contextName, nameConnection)` on the conflict and have the connection probe rebuild,
    which turns the stall back into a working connection to the new cluster.
  - **The catalog's watch is the second thing the stall takes, and it is the one to test.** Every
    refusal stops the subject's watcher (`catalogProbe.Run` calls `unwatch`), and only a run that
    gets a connection stands another up — which, while the conflict holds, never happens. So a
    swapped server costs that cluster its CRD/APIService promptness on top of its sweeps. Nothing
    separate to build: the rebuild above restores both. What it does add is an acceptance
    condition — the recovery is not done when the sweep runs again, it is done when the watch is
    standing again over the *new* connection, which is `ensureWatcher`'s connection comparison
    doing its job rather than anything new.

- **`classify` has no `*apierrors.StatusError` branch.** Every probe reads a raw endpoint today, so
  a status code is the whole evidence and `statusReason` covers it. **Trigger:** the first probe
  that goes through `Connection.Dynamic`, which returns the API's own reason — classifying that on
  the status code alone discards what the typed half already knows (`state.go` states the rule).
  Every probe reads a raw path today, so nothing has reached it yet.

- **`clusterScheduleWatch.probing` is always false, because the engine publishes only on a pass.**
  `probe.Engine` fires `OnPass` after a pass and nowhere else (`sidecar/internal/probe/engine.go`),
  so the snapshot kubeconn projects into `State` never has `Attempts.InFlight()` set: the pass
  publishes with the run queued and not yet started, and the next publish is the commit, by which
  time it has finished. `clusterSchedule` (`sidecar/internal/clustersvc/clusters.go`) reads the flag
  correctly and a unit test pins that it does — nothing on the live feed ever sets it. **Consequence:**
  a UI's countdown reaches zero and sits there for the probe's whole round-trip with no "checking
  now", which is precisely what the schema says `probing` exists to avoid ("asserted by the
  controller for the in-flight window, not inferred from a missing `nextRequeueAt`").
  **Fix:** publish once more when a run begins — `runProbe` marks `begin` under the lock and could
  snapshot there. **The trap that makes this more than two lines:** `OnPass` is documented as
  serialized per subject, and a begin-publish runs on the worker goroutine, so it can land *after*
  the commit-publish and overwrite fresh state with stale. Whatever ships needs a version or a
  serialization point, not just a second call. Weigh a separate hook against widening `OnPass`,
  since "after every pass" is a contract kubeconn's `publish` reads literally.

- **`probe.Engine`'s `Wake`/`WakeAll` pair reads as one axis and is two.** `Wake(subjectName string, names ...string)` takes named probes on **one** subject; `WakeAll(names ...string)` takes named probes across **every** subject. The variadic means the same thing in both, and `All` varies the argument that is not there — so `WakeAll` reads as "wake every probe" when it means "wake these probes everywhere". Both call sites are correct today: `watchKubeconfig` wants `WakeAll(nameConnection)` (one probe, whole fleet) and `Retry` wants `Wake(contextName, probeNames[:]...)` (one context, every probe) — they are exact transposes, which is what makes the pair easy to reach for backwards. **Fix:** rename `WakeAll` to name its axis (`WakeEverySubject`, or `WakeSubjects`), two call sites plus `engine_test.go` and the `sidecar/CLAUDE.md` wiring line. **Weigh:** the engine is a general leaf and `WakeAll` is the shorter, more conventional spelling; the case for renaming rests on the pair being read together, which is exactly when the ambiguity bites.

- **`probe.Engine`'s run queue has no debounce.** `runQ` and `passQ` are `internal/workqueue`
  queues (`sidecar/internal/probe/engine.go`). **Deduping is not debouncing** — a key waits once,
  but only while it is waiting, and one added while a worker holds it is queued afresh on `Done`,
  so asks spread across a run are a run apiece. For the connection probe each one re-reads the
  kubeconfig and the CA files behind it, then dials `/api`.
  - **Four producers reach one context's key today**: `Acquire` → `engine.Add` (once, per new
    context), `watchKubeconfig` → `WakeAll(nameConnection)` (every claimed context, per kubeconfig
    change), `Retry` → `Wake` (all five probes, on demand), and the engine itself — the data edge
    on a committed value, and the subject timer arming the next due pass.
  - **Not a problem yet**, and one throttle already exists: `WithWorkers` caps runs in flight
    fleet-wide, which is what holds back the first pass over a large kubeconfig so every cluster's
    credential helper does not run in the same second. It bounds concurrency, not the number of
    runs. The producers are also quiet — a claim asks once per new context, and `kubeconfig.Service`
    polls on a ticker and publishes only when the whole loaded config differs (`reflect.DeepEqual`),
    so a hand-edited file produces one signal rather than one per write.
  - **The trigger has partly fired.** `Retry` is the third producer and the first *user-driven*
    one: a client that retries on a timer, or a user clicking through an outage, asks for one
    context repeatedly, and dedup merges those asks only while the key is waiting. Still bounded by
    what a person can click, so this is a watch item rather than work — but the next producer that
    can ask in a loop makes it real.
  - **Home:** `internal/workqueue`, as its own feature. The old pairing with `AddAfter` is gone:
    the engine schedules delayed work with a per-subject `time.AfterFunc` over a schedule derived
    in `pass`, so nothing wants a delayed queue add any more.
    [amorey/gobus#17](https://github.com/amorey/gobus/issues/17) proposes the same queue upstream;
    this package is the worked shape to draw from if it lands.
  - **Weigh the alternative first:** a floor belongs to whoever knows what a run costs, and that is
    the engine (it owns the cadence, the backoff, and `WithWorkers`) rather than a generic queue. A
    per-probe minimum interval — "this probe runs at most once every N" — would cover the same
    burst without a second timing mechanism in the queue.

## Auth

- **OAuth access-token refresh — background/proactive half.** On-demand refresh is done (`sidecar/internal/auth/grant.go` refreshes a lazily-expired token using the stored refresh token). What remains: a proactive/background refresh before expiry rather than only refreshing when a consumer hits an already-expired token.
- **SSO failure didn't retry.** The async login tail (wait-for-redirect → exchange → verify → persist) is fire-and-forget; a tail failure is only logged and leaves the session signed-out (a known v1 limitation), with no retry. The user must manually re-initiate login.
- **Check RBAC permissions?** The `ClusterPermissions`/`ResourceRule`/`NonResourceRule` types and schema exist, but the `Permissions` resolver is a stub that returns `not implemented: permissions`. Implement it via a `SelfSubjectRulesReview`. Distinct from the `SelfSubjectReview` *authentication* probe the schema documents on `ClusterPrincipal.username`, which is the rebuild's job.

## Sidecar (Go)

- **Hoist `Condition`/`Event`/`Schedule`/`ObjectRef` when a second consumer appears.** All four are kind-agnostic on the wire (unprefixed, per the schema's naming rule) but live in `internal/clustersvc` (`shared.go`) because the cluster surface is their only consumer. **Trigger:** the first non-cluster kind or subsystem that needs conditions, events, schedules, or owner refs — at that point move all four into a shared leaf package (e.g. `internal/apimeta`), leaving `clustersvc` its `ConditionType` constants (`Connected`/`Healthy`/`Synced`/`Discovered`). `ObjectRef` takes `toOwnerRef` with it. Hoisting earlier would be a one-importer abstraction.

- **Elevate `startCloser` and the start/stop/close helpers to a sidecar-level package.** `internal/clustersvc` defines an unexported `startCloser` — `Start(ctx) (stop func(ctx) error, error)` + `io.Closer` — and composes with `startAll`/`stopAll`/`closeAll`: started in order, stopped and closed in reverse, every error joined, and a failed start unwinding whatever already runs. Every other subsystem already has that exact shape (`poke.Service`, `auth.Service`, `cloud.Service`, `clustersvc.Service`, and `app.App` itself), so `app.Start` hand-writes what the helpers already do. **Fix:** move the interface and the three helpers into a shared leaf (e.g. `internal/lifecycle`) and have `app.Start` compose through them.
  - **Concrete motivation, not symmetry:** `app.Start` has the bug `clustersvc.Start` just fixed — if `cloudSvc.Start` fails it returns an error and *no* stop func, leaving the poke bus and the entire cluster service running with nothing able to drain them. `startAll` makes that unwind structural instead of something each composition root has to remember.
  - **Check before folding:** `app.Close` does more than "release what the drain left" (it stops the gRPC transports *and* closes the cluster store), so confirm each subsystem's `Close` really is the third phase rather than a second stop. The interface also has to be exported, so it needs a name that reads outside `clustersvc`.

- **Hoist the doubling-backoff ladder into a shared leaf when a second consumer appears.** Only `prefsync`'s `backoffDelay` (`internal/cloud/prefsync/engine.go` — `baseBackoff << attempt`, clamped to `maxBackoff`, then jittered, with a `withBackoff(base, max)` test seam) computes one by hand now: `kubeconfigImporter`'s retry went away with the importer, since a `ClusterSource` pass that fails rides beehive's own per-object ladder. **Trigger:** the next thing that cannot ride beehive's — anything outside the control plane, which is what `prefsync` is. At that point extract base/max/jitter and the `Reset`-on-success discipline into a leaf (e.g. `internal/backoff`) with the same parameterized-cadence seam the testing conventions require. Note the two readings a shared type has to keep expressible: `prefsync` counts attempts across reconnects, where a pass-oriented ladder re-levels on any clean pass.

- **Revisit the per-timestamp field resolvers — a zero-aware `Time` scalar could retire them.** Three fields resolve through hand-written resolvers whose only job is mapping the zero `time.Time` (an absent timestamp) to `null` instead of serializing `0001-01-01`: `ClusterCachedDataEvent.firstSeen`/`lastSeen` and `ClusterCachedDataObject.creationTimestamp` (all via `nilIfZeroTime` in `graph/util.go`, wired as `resolver: true` in `gqlgen.yml`). The records deliberately keep **value** `time.Time` (not `*time.Time`) because the delta watches diff frames with `==`, which requires the structs stay `comparable` — so the null-mapping can't just be a nullable pointer field. But the same trick used for the object body — a **custom scalar that does the transform at marshal time** — applies here: bind the GraphQL `Time` scalar (or a new `Timestamp`) to a small comparable wrapper over `time.Time` whose `MarshalGQL` emits `null` for the zero value, then all three fields **autobind** and the three resolvers + `nilIfZeroTime` delete. Catch to weigh: `Time` is currently bound to plain `string` in gqlgen and marshaled by gqlgen's built-ins across the *whole* schema (not just these three fields), so this means either introducing a **separate** scalar for the nullable-zero case or migrating every `Time` field onto the wrapper — check nothing else relies on the string binding before committing. Low priority; purely an internal simplification with no wire change.

## Frontend (webview)

- **CRD printer columns — render a CRD's `additionalPrinterColumns` client-side.** The per-kind column registry (`src/components/widgets/object-columns.tsx`, keyed by `gvrKey`) carries hand-written accessors for **Pod and Deployment only**, and every unregistered kind falls back to the universal Namespace/Name/Age columns — so **CRDs show no kind-specific columns at all**, even though they declare exactly what they want shown. **Approach (settled in the native-body design):** the server ships the column **descriptors** only — `{name, jsonPath, type, priority}`, static per kind, cheap, **no** per-object cell values — and the frontend evaluates the jsonPath client-side against each object's `rawJSON`. So the sidecar still computes no cell values (the whole point of shipping the native body) yet CRD columns work.
  - **Server half is part of the cluster rebuild:** the descriptors come from a CRD's `spec.versions[].additionalPrinterColumns`, discovered per kind and surfaced on `ClusterCachedDataKind` (per-kind and low-churn, so they ride the kinds watch, not the objects watch).
  - **Frontend:** `columnsForKind` becomes a two-tier lookup — hand-written registry entry first, else descriptor-derived columns, else universal-only. Needs a **minimal jsonPath evaluator**: Kubernetes only permits a restricted subset (`.spec.replicas`, `.status.conditions[0].type`), so a ~30-line reader beats a dependency; decide explicitly rather than pulling in a general JSONPath lib.
  - **Respect `priority`:** kubectl hides `priority > 0` columns unless `-o wide`. Filter to `priority === 0` initially; a "wide" toggle on the table can surface the rest later.
  - **Related gap (built-ins):** obvious next kinds are StatefulSet, ReplicaSet, Node, Service, Job, CronJob, PVC. Note **DaemonSet cannot reuse the Deployment/workload accessors** — it has no `spec.replicas`; its Ready is `status.numberReady/status.desiredNumberScheduled`.

- **React compiler — adopt the ESLint plugin.** The Vite/babel transform is already configured (`vite.config.ts`, `babel-plugin-react-compiler`). What remains: add `eslint-plugin-react-compiler` to the lint config.

- **`statusOf` re-derives a verdict the sidecar already folded — this is a bug, not a cleanup.** `cluster-sync-panel.tsx` decides `Paused` locally from `spec.syncEnabled`, ignores the rollup's `status` field entirely, and switches on `reason` with a non-exhaustive list whose default is the healthy-looking green `Syncing`. Consequences: a migration-superseded cache (sidecar says `status=False, reason=Paused` while `spec.syncEnabled` is still true) falls through and renders green; and any new reason the sidecar adds renders as healthy — the opposite of the "an unfamiliar spelling is DEGRADED" rule the rollup is meant to encode. **Fix:** drive the column off `health.status` + `reason` only (`True`→ok, `Unknown`→attention, `False`→per-reason label) with an explicit `Paused` case and a *degraded* default, keeping the local `spec.syncEnabled` check only as the pre-rollup placeholder for a row with no active cache yet.

- **Evaluate moving provenance down into `useWatchSubscription` (keyed on urql's operation key).** `useCacheDeltaWatch`'s provenance is a caller-derived string compared against server-echoed fields (`cacheID` + `apiVersion`/`resource` on `ClusterCachedDataObjectWatchFrame`). But one layer down, `useWatchSubscription` already computes `createRequest(query, variables).key` — which changes *exactly* when the watch's variables change, i.e. it is precisely `(cacheID)` for kinds/events and `(cacheID, apiVersion, resource)` for objects, derived automatically for any subscription. And that file already implements this same tag-fold-mask pattern for the sibling dimension (`generation`, for reconnects). **Fix:** make `Generational<Result>` carry `{ generation, key, result }` and gate both the reducer's fold and the exposed-`data` mask on `key` as well — then `currentProvenance`, `DeltaFrame.provenance`, `joinProvenance`, *and* the `apiVersion`/`resource` schema echo fields all disappear, and every future keyed watch gets staleness protection for free instead of re-spelling it. **Weigh first:** server-echoed provenance additionally defends against a genuinely *mis-routed* frame (a sidecar/host-bridge multiplexing bug), which a client-side key cannot — nothing currently claims that as a motivation, so if it *is* wanted, say so where the fields are defined, since it's the only thing justifying the schema surface. Also note the current design's failure mode is silent: a mismatch between the two hand-spelled provenance strings drops every frame with no type-level protection.

- **Extract a shared delta-watch test harness; mock `useActiveCluster` instead of its inputs.** `cluster-cached-data-events.test.tsx`, `cluster-cached-data-objects.test.tsx`, and `dashboard-nav.test.tsx` each carry a near-identical block: `vi.hoisted` mocks for urql's `useSubscription` + `createRequest`, a `statusState`/`pushReset` transport-status fake, a **re-implementation of the real `applyChange`** inside `vi.mock('@/lib/clusters')`, a `clusterFixture(hasCache, cacheId, serverUid)`, `pushFrame`, and the same `beforeEach`. Two costs: the fake `applyChange` is a second implementation of `src/lib/clusters.tsx`'s real one and can drift from it while tests still pass; and each file still mocks the **pre-refactor** seams (`clusters` + `active-kube-context`) when `useActiveCluster()` is now exactly the seam they want — mocking it deletes `clusterFixture` and the `active-kube-context` mock from all three. Also: `useCacheDeltaWatch` has no test of its own, and the provenance straggler/swap cases are duplicated across the events and objects suites; they could collapse into one `use-cache-delta-watch.test.ts`.

- **Two components are placed where they are to dodge a transport gap.** urql shares one operation between identical subscribers and its replay is query-only, so a second subscriber to a live subscription sees nothing until the next frame. The panel works around this twice by structure — hoisting `useCacheContents` to `ClusterRow` and `useCachedCatalogs` to the panel — enforced only by two long comments, and the second forces a fleet-wide catalog subscription to stay open for the dialog's whole life. **Cost:** component placement is load-bearing and silently re-breakable by any future consumer. **Fix:** put replay in the layer that owns the connection — have `subscribe-exchange.ts`/`useWatchSubscription` keep the latest value per `operation.key` and hand it to a late subscriber on attach.

- **The delta folds are O(N²) over an on-subscribe burst.** `applyChange` copies the whole accumulator map per frame, and each consumer rebuilds and re-sorts a full array from it — but the `Added` snapshot arrives one frame per object. **Cost:** a 5k-object kind is ~12.5M map writes and 5k full re-sorts before the table first paints; the ~150-record `useCachedResources` burst has the same shape. **Fix:** coalesce frames before reducing — accumulate into a mutable `Map` held in a ref and publish a new snapshot once per microtask/animation-frame flush, bumping a version counter the memo keys on. The provenance guard is unaffected.

- **`clusters.tsx`'s join memo rebuilds every cluster on every health frame.** The memo depends on `healthMap`, so a sync-health frame (re-emitted per changed cache on a periodic tick) produces new identities for *all* clusters and *all* joined caches, invalidating every downstream memo keyed on them; the cache lookup inside is a linear `caches.find` per cluster on top. **Fix:** build a `Map<clusterID, cache>` once, and split the join — memoize cluster+cache on `[clusterMap, cacheMap]` only, then attach `health` per row in a child subscribing to just its cache's entry, so one moved cache re-renders one row.

- **`useCachedResources` folds ~150 records into a map and array to yield one id.** Its only consumer, `timelineSyncFor`, reads a single `{ id, resource }`. **Fix:** have `useCachedResources(cacheId, health)` resolve and return that record directly, absorbing `timelineSyncFor`, so `SyncDetail` receives one value rather than a list it immediately collapses.

- **`cluster-sync-panel.tsx` is ~1140 lines holding five separable units.** Eleven `graphql()` documents and six subscription hooks, a status/tone/formatting vocabulary, and three independent panels (`ConnectionDetail`, `SyncDetail`, `ClusterRow`) plus the dialog shell. `overallTone` is exported only because the test file has nothing narrower to import. **Fix:** a `cluster-sync-panel/` directory — `subscriptions.ts`, `status.ts`, `connection-detail.tsx`, `sync-detail.tsx`, `cluster-row.tsx`, `index.tsx` — leaving the panel file as the ~110-line shell. Pure churn, so worth doing when the file is next touched substantively rather than on its own.

- **Investigate consolidating the small `src/lib` modules into a shared `util.ts`.** `src/lib` has grown a tail of very small files — `gvr.ts` (28 lines), `platform.ts` (33), `dashboard-nav.tsx` (41), `active-cluster.tsx` (48), `error-bus.ts` (58), `connection-status.tsx` (60), `window-maximized.ts` (62), `active-kube-context.tsx` (65) — and the per-file navigation/import overhead is starting to outweigh the separation. **The distinction to make first, before moving anything:** most of these are *not* generic utilities, they're cohesive domain modules that happen to be short — `active-cluster.tsx`, `dashboard-nav.tsx`, `active-kube-context.tsx`, `connection-status.tsx` are React hooks/providers with a clear single responsibility and their own consumers, and folding those into a grab-bag would trade a real boundary for a line count. The genuine candidates are the **pure, dependency-free helpers**: `gvr.ts` (a type + one key function) and `platform.ts` (sync `isMacOS()`/`isLinux()` UA checks); `window-maximized.ts` and `error-bus.ts` are borderline. **Tradeoffs to weigh:** a shared `util.ts` is a magnet — it accretes unrelated helpers, blurs ownership, and (if it ends up importing React/urql/domain modules) can create import cycles that the current leaf files structurally can't; against that, a dozen 30-line files means more files to open to follow one flow. **Output:** either a small `util.ts` holding only the pure leaf helpers (with a rule for what may go in), or a decision to leave the split as-is and treat file count as acceptable — recorded either way so this doesn't get re-litigated.

- **Investigate urql caching — are we using it correctly?** We've never audited how the urql client is configured (`src/lib/graphql/client.ts`) w.r.t. caching. Confirm which cache is in play (the default **document cache** vs. `@urql/exchange-graphcache`) and whether it fits our access patterns: queries/mutations over Tauri IPC (`invoke-fetch.ts`), and the many delta-watch subscriptions that maintain their own reduced state via `useWatchSubscription`. Questions to answer: does the document cache help or just add staleness for our mostly-subscription data; are mutations correctly invalidating/refetching the right queries (e.g. cluster enable/sync toggles vs. `clustersWatch`); is `requestPolicy` set intentionally; and would normalized caching (graphcache) actually buy us anything given watches already own the live state. Output: a short note on the current behavior + whether to change the exchange pipeline or leave it.

- **Startup URLs don't reference the active kube context.** A fresh window lands on a bare `/chat` (`index.tsx` redirects `/` → `DEFAULT_ROUTE` with no search); `useActiveKubeContext` resolves the context by *falling back* to `kubeConfig.currentContext` but only *writes* `?kubeContext=` on an explicit pick. Consequence: the landing URL isn't self-describing or deep-linkable until the user interacts, and two windows on different default-resolved contexts look identical in the URL. Fix: seed `kubeContext` from the resolved current-context at boot (e.g. `index.tsx` redirect or an `_app` `beforeLoad`). Catch: at `beforeLoad` the clusters stream may not have delivered its first frame, so current-context isn't known synchronously — either accept a sometimes-omitted param or resolve+write once after the first frame lands.

## Testing

- **Keep the no-wall-clock rule as the rebuild lands.** Both `CLAUDE.md` files state it, and the tree is currently clean: the frontend suites use `vi.useFakeTimers()` + `advanceTimersByTimeAsync`, `waitFor`, or the `flush()` helper with no `setTimeout` waits; Go has **no** literal `time.Sleep` left (the one instance went with the cache system, as did the thin-margin `cache_idle_timeout_test.go`); and `src-tauri/.../sidecar/ipc.rs`'s retry test — `#[tokio::test(start_paused = true)]`, letting tokio auto-advance virtual time between parked timers — is the shape to match. **What to watch for as the sync path returns:** not `time.Sleep` but *thin real-timer margins* — tests that never sleep yet still fail on a loaded machine because they race short real durations. The fix shape is an injectable clock/timer seam so the test advances virtual time. The ~20 `time.After(...)` uses in sidecar tests are almost all *deadlines* guarding a channel receive, which the rule explicitly permits; keep those separate from any load-bearing wait. A `-count=20` soak on a loaded machine is the cheapest way to find regressions.
