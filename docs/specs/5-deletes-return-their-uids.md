---
title: A delete returns the uids it removed
scope: sidecar
status: Planned
order: 5
---

# A delete returns the uids it removed

**Needs:** nothing; the `json_each` idiom it deletes with has landed. The cascade deletes take the returned uids as
one bound list. **Hands on:** constant statement text for the sweep, which
[spec 6](6-prepared-statement-cache.md) can hold.

## Goal

Stop evaluating the relist prune's predicate five times.

`sweepObjects` (`kubestore/objects.go`) deletes an object set and its four side-table rows by
running the same subquery inside each cascade delete:

```go
sub := `SELECT uid FROM objects WHERE ` + where
for _, c := range cascadeTables {
    q := `DELETE FROM ` + c.table + ` WHERE ` + c.uidCol + ` IN (` + sub + `)`
```

The predicate is evaluated four times before the objects row is deleted at all, and the statement
text is composed from the predicate and the table name, so no two of the five are alike.

**`sweepObjects` has one caller** — the relist prune in `ReplaceSession.Commit`, which passes
`updated_at < ?`. `ClearKind` is not one: it hand-writes its own four statements and is out of
scope here, for a reason the design records below.

## Design

The objects delete goes **first**, with `RETURNING uid`, and the cascade deletes take the uids it
returned as one JSON list:

```sql
DELETE FROM objects WHERE api_version = ? AND kind = ? AND updated_at < ? RETURNING uid
DELETE FROM labels WHERE uid IN (SELECT value FROM json_each(?))
```

One predicate evaluation instead of five, and the count comes off the returned rows rather than
`RowsAffected`.

**`extraWhere` goes with it.** The predicate is the last thing in this path still concatenated at
call time, and leaving it there would hand [spec 6](6-prepared-statement-cache.md) a statement it
cannot name. With one caller there is one predicate, so `sweepObjects` carries it rather than
receiving it, and every one of the five statements is then constant text.

**Draining the `RETURNING` cursor to completion is an invariant, not hygiene.** The delete runs to
completion whether or not its rows are read — abandon the cursor after one row and the table is
still empty — so a short read yields a short uid list, the cascades skip the rest, and the
orphaned `labels`, `owner_refs` and `status_history` rows are left behind **with no error from
anywhere**. Today's shape cannot have this bug, because each cascade re-derives the set itself.
So the loop reads every row and checks `rows.Err()`, and a failure there fails the sweep.

**The signature moves.** `sweepObjects` takes `execer`, which is `ExecContext` alone; `RETURNING`
needs `QueryContext`. Its one caller passes a `*sql.Tx`, which supplies both — so this wants a
two-method interface beside `execer` and `querier`, not a widening of `execer` that every write
helper would then carry.

**Deleting the parent first is safe here**, and is the only order `RETURNING` allows. Nothing
references `objects` — `owner_refs`, `labels` and `status_history` are uid-keyed tables with no
foreign key — so there is no constraint to violate and no cascade to trigger. The whole sweep runs
inside the caller's transaction, so a failure after the objects delete rolls all of it back.

`cascadeTables` keeps its shape, including `owner_refs` twice: a deleted object is both a child
and an owner, and with orphan deletion its children outlive it. **That shape is why `ClearKind`
must not be routed through here.** It deletes `owner_refs` by `child_uid` only, deliberately and
with its reason recorded — an edge is extracted from the child's `ownerReferences`, so a retained
child's edge into a cleared owner is still what that child says. Sweeping it by `owner_uid` too
would delete an edge no rewrite of the child can put back — and
`TestStoreClearKindKeepsARetainedChildsEdgeIntoTheClearedKind` already pins that, so the
conflation goes red rather than quietly losing the edge.

A sweep that matches nothing marshals to `null`, and `json_each('null')` in a `WHERE uid IN (…)`
matches nothing rather than erroring — the reverse of the same value's effect in
the edge-table inserts, where it is fatal. Benign here, and worth knowing that the
difference is the statement, not the value.

## Rules

- **The predicate runs once.** A second statement that re-derives the same set has undone this.
- **The uid list is bound, not interpolated.** Spec 4's idiom, for the same reason.
- **`deleteObjectRow` stays a point delete.** It already knows its uid, so `RETURNING` would tell
  it nothing.
- **A `RETURNING` cursor is drained to the end, and its `Err()` is checked.** A short read orphans
  rows and reports nothing.
- **`ClearKind` keeps its own statements.** Its `owner_refs` asymmetry is the point of them.

## Not in this pass

- **Chunking the returned list.** A full-kind sweep returns every uid — 10k uids is a few hundred
  kilobytes, held for the length of one transaction that is already writing more than that. If a
  cache appears where it matters, chunk the cascade deletes, not the objects delete.
- **Measuring the trade.** The gain claimed here is one predicate evaluation instead of five, and
  that is all it is. Each cascade swaps an indexed scan of `objects` — there are four
  `(api_version, kind, …)` indexes — for an ephemeral index over the bound uid list. Very likely a
  win and irrelevant at ordinary sizes, but at the 10k end it is unmeasured, and the chunking item
  above prices that case in memory alone.
- **The events prune** (`ReplaceSession.Commit`), which deletes from one table with no cascade and
  has nothing to return.

## Build order

1. The two-method interface, and `sweepObjects` moved onto it with `extraWhere` dropped.
2. `sweepObjects` returns the uids: objects deleted first with `RETURNING`, drained fully, cascades
   by bound list. **Write the cascade test first — the path has none.** Every side-table
   assertion in `store_test.go` belongs to `ClearKind`; the two prune tests assert object counts
   and the pruned number and nothing else, so the behaviour this step rewrites is uncovered. The
   test: a swept object's `labels`, `owner_refs` and `status_history` go with it, in both
   directions of the `owner_refs` pair.

   **The fixture needs several objects.** Both existing prune tests remove exactly one, and with
   one row to return there is no remainder to abandon — a partial drain sails through them.
3. `RowsAffected` goes from that path.

## Done when

The relist prune evaluates its predicate once, every side-table row of a swept object goes with
it, and none of the sweep's five statements has text assembled at call time. `ClearKind` is
unchanged. Delete this spec when step 3 lands.
