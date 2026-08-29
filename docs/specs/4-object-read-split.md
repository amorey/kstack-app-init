---
title: Object read split
scope: sidecar
status: Planned
order: 4
---

# Object read split

**Needs:** nothing. **Hands on:** nothing. Fourth because nothing is broken — the objects watch
is correct and costs far more than it should. A cheap change with no wire consequence, so it can
be built out of turn.

## Goal

Stop the objects watch retaining, and re-reading, every cached body.

`runCachedDataWatch` (`cacheddatawatch.go`) keeps `prev map[string]T` — the whole collection —
and on each debounced burst re-reads all of it and diffs by key. For objects `T` is
`kubestore.ObjectRow`, which carries `CompressedJSON` (`reads.go:166`), so a watch on a
10k-object kind holds every compressed body in Go memory and pulls all of them back out of
SQLite every 250ms (`dataObjectsDebounce`, `cacheddata.go:147`) to learn that three rows moved.

**Nothing in the diff reads a body.** `key` is the uid and `changed` compares `ResourceVersion`
alone (`cacheddata.go:258`, `:262`). A body is needed only by the rows that become `Added` or
`Modified` frames — and on `Deleted` not even then: `applyChange` (`src/lib/clusters.ts:155`)
does `next.delete(id)` and never looks at the entity it was handed.

So the read is split: identity for the diff, body on demand.

## Design

Three changes. No migration, no schema change beyond one already-nullable field going null.

### 1. `ObjectRow` loses the body

`Store.Objects` drops `raw_json` from its SELECT; `ObjectRow` loses `CompressedJSON` and
`Body()`. The body is fetched by uid instead:

```go
// ObjectBody is one object's stored body, decompressed. Separate from the row because the
// watch diffs on (uid, resourceVersion) and only the rows that become frames need one.
// The bool is false when the row is gone — deleted between the diff read and this call.
func (s *Store) ObjectBody(ctx context.Context, uid string) ([]byte, bool, error)
```

`Objects` has one production caller — the watch (`cacheddata.go:264`) — so there is no read path
that still wants whole rows, and no second query to keep in step. Of the test callers only
`reads_test.go:158` wants a body, and it moves to `ObjectBody` with the assertion it was making.

The memory saving falls out of the struct rather than out of the loop: `prev` holds the same
rows, now identity-sized. **The snapshot's transient peak drops with it** — `read` returns a
slice that no longer has the collection's bodies in it.

### 2. The frame builder gets the store

`frame func(DeltaFrameType, T) F` widens to `frame func(context.Context, *kubestore.Store,
DeltaFrameType, T) F`, and `sendDiff` passes the store it already holds. Objects fetch a body for
`Added` and `Modified`; kinds and events ignore both new parameters.

Widen the one hook rather than adding an optional `hydrate` beside it: an optional hook is a nil
branch on every row, for two watches that would never set it.

**A body that will not load serves `rawJSON: null` and the watch continues.** That is already the
policy for a decompression failure (`cacheddata.go:349-352`), and it now also covers the row
being deleted between the diff read and the fetch — where the next resync's `Deleted` frame is
the real answer, and failing the watch would be reporting a race as a breakage. `rawJSON` is
nullable (`schema.graphqls:728`), so a null body does not null its parent frame.

**The row can also be updated in between**, which one SELECT made impossible: the diff read
observes `resourceVersion` N and the fetch returns the body at N+1, so a frame carries identity
from one version and a body from another. Nothing observable is inconsistent — name, namespace and
kind are immutable per uid — and it converges, because that write pinged the bus and the next
burst diffs N+1 against a `prev` holding N and sends a `Modified` with a body no older than that
— the ping surviving a re-read in progress is `bind`'s subscribe-before-read invariant
(`cacheddatawatch.go:228`), which the fetch sits inside rather than outside.
Worth stating rather than discovering: two queries against a moving table is the objection the ADR
below raised, and this is its whole content.

### 3. `Deleted` carries no body

`toCachedDataObject(r, nil)`. **The frame's `object` must stay non-null** — the client reads the
uid off it to key the removal, and a change with no entity is dropped rather than folded, so an
absent object would leave deleted rows on screen until the next reconnect. Only `rawJSON` goes
null, and the frontend already tolerates that (`object-columns.tsx:37` reads `o.rawJSON ?? {}`,
and a deleted object is removed from the map before anything renders it).

### `changed` can go

With the body gone `ObjectRow` is comparable, and `a != b` is exactly the right test: the server
moves `resource_version` on every write, so no change escapes it, and a relist rewriting a row
unchanged compares equal and sends nothing. The other two watches already pass `a != b`.

So the hook deletes and the spec's constraint becomes `T comparable`. **Take it** — it is the
simplification the split pays for, and the comment explaining why the hook exists says in as many
words that the body is the only reason it was ever needed.

## The ADR this reopens — decide it in this pass

[ADR: cached-data reads](../adr/2026-08-26-cached-data-read-loop.md) declined this split by name,
in its consequences:

> Reading keys first and fetching bodies only for the changed set would fix that; declined for
> now, because it doubles every read into two queries against a moving table for a saving we have
> not measured.

Two sentences in its decision go with it: that the read "hands `raw_json` back as stored and only a
row that becomes a frame is decompressed", and the account of why the field is spelled
`CompressedJSON`.

**Write a new ADR for the split and leave that one Accepted.** Its decision — ping, re-read, diff;
a cache that went away ends clean; a read claims rather than borrows — is untouched, and that is
most of the document. Flipping it to `Superseded` would tell a reader the read loop's design
changed when only one field's mechanics did, and would oblige the new ADR to restate four
paragraphs that still hold. Append a pointer to the new ADR under the paragraph above; leave its
own words alone, since it records what was believed then, and "declined, unmeasured" was true.

The new ADR carries the three calls a reader would otherwise re-litigate: the `Deleted` frame's
null body, widening `frame` rather than adding an optional `hydrate`, and the covering index
declined with its reason. `docs/adr/README.md` gains a row; `sidecar/CLAUDE.md` gains the link
beside the one it already carries, which keeps working because the old ADR stays Accepted.

## Rules

- **The diff never loads a body.** The moment a body is read to decide whether something changed,
  the split has been undone.
- **One body read per frame, not per row.** `Added` and `Modified` only.
- **A failed or missing body is a null field, never a failed watch.**
- **`Objects` stays bodiless.** A caller that wants whole rows gets its own query rather than
  putting `raw_json` back on this one.

## Not in this pass

- **A covering index** on `(api_version, kind, uid, resource_version)`, which would make the diff
  read index-only instead of one row fetch per uid. Tempting and probably wrong: `resource_version`
  moves on every write, so the index would be rewritten on the hottest table's hottest path to
  save page fetches on a read that runs at most four times a second per watch. It also needs a
  migration (`0002_*.sql`), which nothing else here does. Measure before adding it — and note the
  fetches skip the body entirely, since SQLite keeps a large blob in overflow pages the scan never
  touches.
- **Batching the body fetches.** The snapshot turns one scan into N point lookups on the uid
  primary key — the one cost this split adds, paid once per connection rather than per burst. A
  chunked `WHERE uid IN (…)` is the escape hatch if a large kind's first paint regresses; it is
  not worth the chunking logic on a guess.
- **A write log.** The read split makes each resync cheap but still O(collection) in uids scanned.
  Going to O(changes) means an append-only `object_writes` table, which is a different order of
  work and buys things nothing consumes yet — see the `object_writes` item in [`TODO.md`](../../TODO.md).

## Build order

1. `kubestore`: `Objects` without the body, `ObjectBody`, `ObjectRow` without `CompressedJSON`.
   `TestObjectsReturnTheBodyStillCompressed` (`reads_test.go:147`) becomes the `ObjectBody` test —
   it returns what `Body()` did — and gains that an unknown uid answers false.
2. Widen `frame`, drop `changed`, make `Deleted` unhydrated. `cacheddata_test.go:226` already
   pins that a body is served; add its counterpart — **a `Deleted` frame carries no body** —
   because nothing else would catch a hydrate that fired on every frame type.

   **Steps 1 and 2 are one commit.** `toCachedDataObject` calls `r.Body()` (`cacheddata.go:349`),
   so the tree does not build between them.
3. The new ADR, and the docs the split falsifies:
   - `sidecar/CLAUDE.md:175` — "**The diff takes a `changed` func, not a `comparable` constraint.**
     `ObjectRow` holds the body as stored (`CompressedJSON`) and cannot be compared with `==`."
     Falsified outright, so the bullet is rewritten rather than extended.
   - `reads.go` — `Objects`' "the body comes back compressed" paragraph, and `Body()`, which goes.
   - `cacheddatawatch.go` — the `cachedDataWatchSpec` doc's case for `changed` over `comparable`.
   - `cacheddata.go:104` — "Keeping RawJSON in the struct is what makes an in-place edit differ
     across two reads": the record is not what is diffed, and after this it is not what carries
     the body across a resync either.
   - `gqlgen.yml:187` — the autobind is justified there by "the watch already reads the body for
     every row", which is the premise this spec removes. The conclusion survives; the reason does
     not.
   - `schema.graphqls:727` — `rawJSON` is documented as "resolver-gated", which `gqlgen.yml`
     already contradicts ("no resolver"). Say what is true after the split: the body is read only
     for the rows that become frames.
   - `schema.graphqls:763` — "On `Deleted`, its last-known row" needs to say the body is absent.

## Done when

A watch on a large kind holds identity rows rather than bodies, a resync reads bodies only for the
rows it sends, and a `Deleted` frame carries a null `rawJSON` without the object table missing the
removal.

The new ADR is written and indexed, and `sidecar/CLAUDE.md`'s `changed`-versus-`comparable` bullet
says what is true instead of being amended. Delete this spec when step 3 lands.
