---
title: Bind a list as one argument with json_each
scope: sidecar
status: Planned
order: 4
---

# Bind a list as one argument with json_each

**Needs:** nothing. **Hands on:** the idiom [spec 5](5-deletes-return-their-uids.md) deletes
with, and constant statement text for the two writes that build SQL per row — which is what
[spec 6](6-prepared-statement-cache.md) can hold. Fourth because a statement cache cannot cache
text that is assembled at call time.

## Goal

Stop building SQL whose shape depends on how many values it carries.

`insertObjectRow` (`kubestore/objects.go`) composes two statements per object out of
`valuesPlaceholders(n, 3)`:

```go
q := `INSERT INTO owner_refs (child_uid, owner_uid, is_controller) VALUES ` +
    valuesPlaceholders(len(row.OwnerRefs), 3) + ` ON CONFLICT…`
```

So a Pod with three owner references and eleven labels produces two statement texts no other Pod
produces. Every distinct text is a separate `sqlite3_prepare_v2` — modernc compiles and finalizes
per call and caches nothing — and no statement cache can ever hold one.

## Design

A collection is marshalled to JSON and bound as **one** argument, which the statement expands.
**The two inserts take different shapes, because the two Go values are different shapes** — a
JSON array and a JSON object read out of `json_each` through different columns, and using the
wrong one for either is not a style choice.

`row.OwnerRefs` is a slice, so it marshals to an **array of tuples** and the columns come out of
the element's positions:

```sql
INSERT INTO owner_refs (child_uid, owner_uid, is_controller)
SELECT ?1, value ->> 0, value ->> 1 FROM json_each(?2) WHERE true
ON CONFLICT(child_uid, owner_uid) DO UPDATE SET is_controller = excluded.is_controller
```

**What gets marshalled is `[][2]any`, not `[]ownerRef`.** Marshalling the struct slice is the
obvious move and it is wrong: it produces `[{"UID":…,"IsController":…}]`, where `value ->> 0`
returns NULL without erroring. The schema catches it — `owner_refs.owner_uid` is `TEXT NOT NULL`
in a `STRICT` table — so it surfaces as a constraint violation rather than silent NULLs, but the
shape is decided here.

`row.Labels` is a `map[string]string`, so it marshals to an **object** and `json_each` serves its
own `key`/`value` columns:

```sql
INSERT INTO labels (uid, key, value)
SELECT ?1, key, value FROM json_each(?2) WHERE true
ON CONFLICT(uid, key) DO UPDATE SET value = excluded.value
```

Positional access over an object is not merely empty — `value ->> 0` against a JSON object fails
the statement with "malformed JSON".

**`WHERE true` is load-bearing, not decoration.** SQLite's parser cannot tell an `INSERT … SELECT
… FROM x ON CONFLICT` apart from a `SELECT` with a join constraint; without it the statement is a
syntax error at `DO`. Both statements upsert, so both need it.

**Both `if len(…) > 0` guards stay, and they are load-bearing.** An *empty* array or object
expands to no rows, but the Go zero values are neither: `row.Labels` is `u.GetLabels()`, nil when
the object has no labels, and `row.OwnerRefs` is built by `append`, so it is nil when there are
none. Both marshal to `null`, and `json_each('null')` yields **one row with NULL columns** — a
`NOT NULL` violation under `STRICT`, inside the relist page's transaction, on every object with
no labels and every object with no owner. The guard is what keeps a nil from ever reaching the
statement. Normalizing instead (a non-nil `make([][2]any, 0, n)` marshals to `[]`) would work for
the slice but still needs an explicit nil check for the map, whose nil arrives from apimachinery
rather than from the call site — more code in more places to remove one branch. This spec is
about statement text, which the guard does not touch.

**`boolToInt` goes with its last caller.** JSON `true`/`false` arrives as SQLite integer 1/0,
which satisfies `is_controller INTEGER NOT NULL` under `STRICT`, so the conversion has nothing
left to do. `valuesPlaceholders` goes the same way.

The floor this sets is SQLite **3.38** — `->>` is the newer of the two features, not `json_each`.
The modernc build in use is 3.53.2.

## Rules

- **A statement's text does not depend on its arguments.** A collection is one bound argument,
  never a run of placeholders.
- **One statement per collection, not per element.** Marshalling and then looping is the same cost
  this replaces.
- **The SELECT list follows the JSON shape.** An array reads through `value ->> n`, an object
  through `json_each`'s `key`/`value`. Crossing them is a runtime failure, not a preference.

## Not in this pass

- **Batching the objects watch's body fetches.** The read split turns a snapshot into N point
  lookups on the uid primary key, and a chunked `WHERE uid IN (SELECT value FROM json_each(?))` is
  its escape hatch — but only if a large kind's first paint is measured to regress. See
  [ADR: the objects read split](../adr/2026-08-29-object-read-split.md).
- **`sweepObjects`' composed predicate**, which spec 5 reshapes.

## Build order

1. The `owner_refs` insert over `[][2]any`, with four assertions: an object with several owner
   references round-trips; `is_controller` comes back as an integer 1/0; an object whose
   references *shrink* loses the row, which pins the unconditional delete that precedes the
   insert; and **an object with no owner references at all inserts cleanly**. The last is the one
   that goes red on a nil reaching `json_each`.
2. The `labels` insert over the marshalled map, reading `json_each`'s `key`/`value` columns —
   **not** the statement above with the table name changed. Same round-trip, shrink, and
   empty-collection assertions.
3. `valuesPlaceholders` and `boolToInt` go — neither has a test of its own, so nothing goes with
   them.

## Done when

`insertObjectRow` issues the same statement text for every object regardless of how many
references or labels it carries, and `valuesPlaceholders` and `boolToInt` are gone. Delete this
spec when step 3 lands.
