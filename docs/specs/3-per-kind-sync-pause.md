---
title: Per-kind sync pause
scope: sidecar, frontend
status: Planned
order: 3
---

# Per-kind sync pause

**Needs:** nothing. **Hands on:** nothing. Third because it is new capability rather than a
repair — the other two specs fix things that are broken, this adds something that never worked.

## Goal

Let a user stop syncing one kind without losing what is already cached.

A cache mirrors every kind the cluster serves, which on a cluster with a busy CRD or an
event-storming namespace is not what the user wants. Today the only controls are whole-cluster:
`clusterSyncEnabled` freezes everything, and `clusterCacheClear` throws everything away.

**Pause is not deletion, and the distinction is the whole feature.** Deleting a
`ClusterCachedKind` stops the sync *and* clears the rows (`stopSyncAndClearRows`), and the
discovery sweep recreates the record on its next pass anyway. Pause stops the sync and **keeps
the rows**, so the cached data stays readable throughout and an unpause reconciles into what is
already there.

**Do not promise a cookie resume.** The cookie is a resourceVersion and an API server's watch
history is minutes, so any pause worth making outlives it: the resume takes a 410, `positionGone`
marks a relist, and the kind cold-lists. That is fine — the relist reconciles into the kept rows
rather than starting from nothing, and the rows are readable the whole time, which is the actual
difference from delete. A short pause does resume off the cookie; a long one does not, and the
feature is no worse for it.

## Design

### The field is `Paused`, and the inversion is the point

```go
type ClusterCachedKindSpec struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Resource   string `json:"resource"`
	Namespaced bool   `json:"namespaced"`
	// Paused is the user's switch for this one kind, inverted so the zero value is the
	// default: every stored record predates this field, and a missing JSON key decodes
	// to false.
	Paused bool `json:"paused"`
}
```

**An `Enabled bool` would stop the fleet on the upgrade that shipped it.** beehive stores a spec
as `json.Marshal(spec)` and decodes it back, so a key absent from every existing record decodes
to `false` — every kind in every cache would read as disabled, the controller branch below would
`ForgetKind` all of them, and syncing would stop everywhere, silently, looking like a healthy
paused cache. Nothing would repair it: the no-op-when-unchanged rule below means the catalog
fields match, so the record is never rewritten and the key is never filled in.

beehive supplies the mechanism for this — a `Migrator` with `ConvertSpec`, registered per kind
via `WithMigrator`, converting lazily at the decode boundary. The repo registers none today, so
that is a new mechanism for one field. **Inverting the field costs nothing and needs no
migration**, which is why it wins here.

The wire keeps the positive form, matching the cluster's own two toggles:

```graphql
clusterCachedKindSyncEnabledSet(id: ObjectID!, syncEnabled: Boolean!): ClusterCachedKind!
```

so `ClusterCachedKindSpec.syncEnabled: Boolean!` is served as `!Paused`. One negation at the
projection, and a comment at both ends saying why the stored name is the opposite of the served
one — otherwise the next reader "fixes" the mismatch and reintroduces the upgrade bug.

The setter is `clustersAPI.updateSpec`'s shape exactly — read the spec, apply the edit, write it
back under a mutex, because beehive's `Update` takes the whole spec and has no compare-and-swap.

### The trap: the discovery sweep is a second writer

`upsertKinds` (`caches.go:940`) calls `CreateOrUpdate` per kind on every sweep, with a spec built
purely from the catalog row:

```go
desired[name] = ClusterCachedKindSpec{APIVersion: ..., Kind: ..., Resource: ..., Namespaced: ...}
```

Adding `Paused` to that struct means every sweep writes the zero value over the user's choice —
a pause that silently un-pauses itself within one discovery interval. The `updateSpec` mutex does
not help, because the sweep never takes it.

**Fix: make the sweep a no-op when nothing catalog-owned moved.** Read the stored records once
(`ListOwnedObjects`, which `pruneKinds` already calls), and for each desired kind:

- no record → `CreateOrUpdate` with the catalog fields (`Paused` zero, i.e. syncing);
- record whose catalog fields all match → **write nothing**;
- record whose catalog fields differ (a renamed or re-scoped kind) → write the catalog fields and
  carry `Paused` forward from the stored record, under the setter's mutex.

**Keep the `ErrDeletionPending` tolerance on both writing branches.** `upsertKinds` swallows it
today with its reason attached — a record marked for deletion holds its name until GC releases it,
and a later pass creates the kind again off the same catalog row. The restructure makes it easy to
drop: `ListOwnedObjects` returns deletion-pending records (which is why `pruneKinds` tests
`DeletionRequestedAt` before marking), so they land in whatever name→record map the new code
builds, and a marked record the cluster now serves at a different scope takes the write path and
fails until collection catches up. Behaviour is unchanged either way — between mark and collect,
writing nothing and swallowing the error are equally inert — so this is error handling to carry
across, not a decision to remake.

The third case is the only read-modify-write, and it happens on an actual catalog change rather
than every pass. Fold the list into `pruneKinds`'s existing one so the sweep still reads the
records once.

This is worth doing on its own merits: it removes a per-kind write transaction per sweep on a
cache with hundreds of kinds, which is the steady state.

### The controller

`clusterCachedKindController.Reconcile` gains one branch, before the `TrackKind` call:

```go
if obj.Spec.Paused {
	c.kubesyncSvc.ForgetKind(int64(cacheID), toKubestoreKind(obj.Spec))
	// Before the return, or the pause never reaches the timeline: the logging below
	// this branch is never reached.
	if err := c.logSyncVerdict(ctx, client, cacheID, obj.Spec); err != nil {
		return beehive.Fail(err)
	}
	return beehive.Settled()
}
```

`ForgetKind` joins the worker before returning, and **no `clearKindRows`** — that call is what
makes deletion deletion.

**Calling it every pass is fine.** The branch is level-triggered, so it fires on each reconcile
while the kind is paused, and the whole chain is idempotent: `tracked`'s delete is a no-op for an
absent kind, and `stopKind` either returns on a nil session or bottoms out in
`Supervisor.Remove`, which returns on an absent subject. The cost is one `armMu` acquisition per
paused kind per pass. Not worth transition-tracking state to avoid.

### The verdict a paused kind reports

This is the part that does not fall out for free, and it is where the obvious edits are wrong.
`logSyncVerdict` and the health fold both read `kubesyncSvc.GetKindState`, and a forgotten kind
has no state — `ok` is false.

**The reason comes from the spec, never from kubesync.** The service knows the record; kubesync
deliberately does not know why it was not asked to sync something. So `Paused` is decided ahead
of every `GetKindState` call, not derived from its absence.

**Skip paused kinds before the `!ok` branch, not after the fold.** `readCacheHealth`'s `!ok`
branch sets `anyKindUnanswered` (`caches.go:480`), and the switch's first arm is
`case health.TotalKinds == 0 || anyKindUnanswered:` (`:515`), which leaves the `Connecting`
default and never reaches the other two arms. So filtering paused kinds out of `unhealthyKinds`
and the `lastLiveAt` minimum changes nothing — the cache still reads "connecting" forever, which
is the exact failure this section exists to prevent. The skip has to happen off `kindObj.Spec`, at
the top of the loop, before the state is read at all.

**`readSyncStatus`'s per-kind row has the same shape and the same fix.** It fills
`ClusterCacheKindSyncStatus` only `if state, ok := GetKindState(...); ok` (`caches.go:612`), so a
paused kind's row would carry an empty reason — and the panel's list is where a user actually
looks for it. Set `Reason: ReasonPaused` from the spec before that read. `ObjectCount` is already
outside the `ok` block, so it keeps answering.

**A paused kind still counts in `totalKinds`.** The skip applies to the state read and the
tallies, not to the census — `health.TotalKinds++` stays above the `continue`. `totalKinds` is a
wire field with a documented meaning and three consumers that a shrinking count would break:
`kindsSyncingLabel` renders "118 of 120 kinds syncing" (`cluster-sync-panel.tsx:709`), so pausing
two of five would read "3 kinds" and the two the user just paused would vanish rather than be
accounted for; `:807` gates the whole "Kinds syncing" row on `totalKinds > 0`, which an all-paused
cache would lose entirely; and `schema.graphqls:564` says `totalKinds: 0` means the discovery pass
has not landed, a meaning the field cannot keep if 0 also means "everything is paused".

**So count them separately and put the tally on the wire** — `pausedKinds: Int!` on
`ClusterCacheHealth`, which the fold computes anyway for the arm below. Without it the summary
loses them through the other door: `unhealthyKinds` excludes paused kinds (a paused kind is not
unhealthy), so five kinds with two paused gives `unhealthyKinds == 0`, and `kindsSyncingLabel`
short-circuits on exactly that (`:709`) and renders a plain "5 kinds". The two the user just
paused are invisible either way unless the count is served. With the field, step 4's label can
say "3 of 5 kinds syncing, 2 paused".

**And `pausedKinds` must go into `sameHealth` (`caches.go:761`), or pausing publishes nothing.**
That gauge dedupes per cache against a hand-written field-by-field comparison — hand-written
because `UnhealthyKindRefs` is a slice, so the struct cannot use `==` — which means a field
nothing compares is invisible by default. Take the case the feature exists for: a healthy cache,
five kinds all `Watching`, and the user pauses one. `TotalKinds` holds at 5 because the census is
kept, the paused kind is skipped so `UnhealthyKinds` stays 0 and the refs stay empty, and `Reason`
and `Status` do not move. Every field the comparison reads is unchanged, so the frame is
suppressed and the new count never reaches the webview — the label would keep saying "5 kinds"
until something unrelated moved the rollup.

The sibling gauge needs nothing: `sameKindSyncStatus` compares `Reason` (`:777`), and a paused
row's reason becomes `Paused`, so `WatchSyncStatus` publishes on its own.

**The all-paused cache needs a new arm; there is nothing to reuse.** `ReasonPaused` has exactly
one producer — `readAllCacheHealth`'s short-circuit on `cacheSyncEnabled` (`caches.go:715`), which
fires before `readCacheHealth` is called and is about the *cluster's* switch. `readCacheHealth`'s
switch has three arms (Connecting, Watching, first-offender) and none is Paused.

With the census kept, the arm is plainly expressible — `paused == health.TotalKinds &&
health.TotalKinds > 0` — answering `False`/`Paused`. **It must come first in the switch.** Not
because of the `TotalKinds == 0` arm, which it no longer collides with, but because of the one
after it: paused kinds are skipped, so `anyKindUnanswered` is false and `UnhealthyKinds` is zero,
and a fully paused cache would otherwise report `Watching`.

**The schema has three sites, and they need three different edits.**

- `ClusterCacheKindSyncStatus.reason` (`:625`) is the one a client reads the new value off — the
  field `readSyncStatus` sets from the spec. It enumerates
  (`Syncing`/`Resyncing`/`Resuming`/`Watching`/`Stale`/`SyncFailed`, plus the two connection
  reasons) and has no `Paused`. Add it.
- `ClusterCacheHealth.reason` (`:563`) enumerates nothing — it states a fold rule, and already
  claims "`Paused` speaks only when every kind is paused". Rewrite it to admit a cache-level
  verdict above the per-kind fold, which is what the new arm makes true.
- The overview at `:71` already lists `Paused` among the per-kind `ClusterCachedKind` reasons.
  Leave it — this change is what stops it being aspirational.

Plus `ClusterCacheHealth.pausedKinds: Int!`, new.

**`logSyncVerdict` needs the paused case ahead of its guard.** It returns early on `!ok`
(`cachedkinds.go:395`), and a forgotten kind is precisely `!ok` — so even if the branch reached
it, nothing would be written. It already receives the spec, so it needs no new argument: read
`spec.Paused` first and resolve to `ReasonPaused` with no state read, leaving `GetKindState` to
the unpaused path. A repeated `(Category, Type, Reason)` extends the run, so a kind left paused
costs one row, not one per pass.

**`objectCount` still reports**, because it comes off the store's per-kind counts and not the sync
seam — which is the point of keeping the rows.

## Rules

- **Pause keeps the rows.** The only thing that clears them is deletion.
- **The catalog owns four fields; the user owns one.** Nothing that writes on a schedule may
  write `Paused`.
- **A paused kind is skipped, not folded.** It never reaches `GetKindState`, so it can never be
  read as unanswered.
- **The zero value is the default, and that is a storage decision.** Every stored record predates
  the field, so the default has to survive a missing JSON key without a migration.

## Build order

1. The spec field and the sweep's no-op-when-unchanged path, with no way yet to set `Paused`.
   Test: a sweep over unchanged catalog rows issues no writes; a re-scoped kind converges while
   carrying a hand-set `Paused: true` through; and a record stored before the field decodes as
   syncing.
2. The controller branch and the verdict handling. Test: a paused kind's worker is joined, its
   rows survive, its `objectCount` still answers, its cache's health does **not** read
   `Connecting`, a `Paused` run lands on its timeline, and it is counted in `totalKinds` and
   `pausedKinds` both. Then the dedupe: pause one kind on an otherwise-idle healthy cache and
   assert a health frame arrives — nothing else about that cache moves, so this fails until
   `sameHealth` knows the new field.
   Then the arm ordering, which is the easy thing to get wrong: a cache whose every kind is paused
   answers `Paused`, not `Watching`.
3. The mutation and the schema — the three doc sites plus `pausedKinds` — then `pnpm codegen`.
4. The webview control, plus `kindsSyncingLabel` spending `pausedKinds`, and the offender list
   has to learn about `Paused` first. `failingKinds`
   keeps every row whose reason is outside `SETTLING_REASONS` — `{Watching, Syncing, Resyncing,
   Resuming}` (`cluster-sync-panel.tsx:755`, `:777`) — so the `Paused` rows step 2 adds would all
   land under the heading "Kinds not syncing" (`:849`). Presenting a kind the user just paused as
   a fault is the same false alarm the reason-from-spec rule prevents one layer down.
   **Give paused kinds their own section rather than adding `Paused` to `SETTLING_REASONS`**: the
   list is where the unpause control belongs, and folding them into the settling set would hide
   them instead of making them actionable.

Steps 1 and 2 are testable with no UI and no mutation; step 1 is worth landing alone whether or
not the rest follows, since it deletes the per-sweep write.

## Not in this pass

- **Pausing a whole API group.** A per-group switch is a different record, and the per-kind one
  is what the sync panel can already express.
- **A pause that survives the kind disappearing.** A kind the cluster stops serving is pruned,
  and its pause goes with the record. Remembering it would need a separate durable set keyed by
  GVR, which is only worth it if someone hits the case.
- **Auto-pause on cost.** Pausing a kind because it is expensive is a policy, and nothing
  measures per-kind cost yet.

## Done when

Pausing a kind in the sync panel stops its watch, leaves its cached objects listed and readable,
leaves its cache reading healthy rather than connecting, and survives a discovery sweep. A cache
with every kind paused reads `Paused`. Unpausing brings the kind back — off the cookie if the
pause was short enough for the server to still serve it, otherwise by relisting into the rows it
kept — with the cached data readable throughout.

`sidecar/CLAUDE.md`'s cached-kind section gains the ownership split in the same commits. Delete
this spec when step 4 lands.
