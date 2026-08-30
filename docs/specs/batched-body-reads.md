---
title: A burst of object frames reads its bodies in one statement
scope: sidecar
status: Planned
---

# A burst of object frames reads its bodies in one statement

**Needs:** nothing. **Hands on:** nothing — the wire is unchanged, and no client can tell.

## Goal

Stop paying one query per row for the bodies of a burst.

The objects watch reads identity only and fetches a body per frame: `hydrateObject`
(`clustersvc/cacheddata.go`) calls `Store.ObjectBody(ctx, uid)` for every `Added`/`Modified` frame
it builds. That split is right — the diff must not load bodies, and a `Deleted` frame needs none
(→ [ADR: the objects read split](../adr/2026-08-29-object-read-split.md)) — but the fetch is a
point query per row, so a cold snapshot of a 5,000-Pod kind is 5,000 round trips through the
reader pool, and a relist that moves every row is another 5,000.

Nothing about that is per-row work: the frames of one burst are known together, before any of them
is built.

## Design

**One read per burst, not per frame.** The loop already has the whole batch in hand — the
snapshot's rows, or a changes read's `Written` — so it fetches their bodies together and hands each
frame the one it needs.

```go
// Bodies for the uids that have one, keyed by uid. A uid with no row is left out: it went
// between the read that named it and this call, which the next resync reports as a Deleted.
func (s *Store) ObjectBodies(ctx context.Context, uids []string) (map[string][]byte, error)
```

The statement is the batched-uid shape the deletes cascade already uses — one bound JSON argument,
never a run of placeholders, so the text does not vary with the count and modernc compiles it once
(`sidecar/CLAUDE.md`, the kubestore section):

```sql
SELECT uid, raw_json FROM objects WHERE uid IN (SELECT value FROM json_each(?))
```

**Decompression stays per row**, as it is now: it is CPU against a body that is about to be
serialized anyway, and batching it buys nothing.

**The frame builder stops fetching.** `cachedDataWatchSpec.frame` is handed the store precisely so
the objects watch can fetch what the diff did not read; it would instead be handed the batch's
bodies. The shape that keeps the other two watches unaffected is to leave `frame` alone and give
the spec an optional `prefetch func(ctx, s, rows []T) any` whose result is threaded to `frame` —
nil for kinds and events. Decide that against the alternative of hydrating inside `sendDiff`/`apply`
when the code is in front of you; what must not happen is a second loop.

**A cap, because a burst is unbounded.** A relist of a large kind arrives as one batch, and one
`IN` list of 200,000 uids is a statement SQLite has to parse and a map the process has to hold.
Chunk at a fixed size (500 is the relist page size the sync already writes in) and read chunk by
chunk. The frames go out as they do today — one per row, in order — so the cap changes nothing a
client sees.

**A body that will not load is still a null field, never a failed watch.** That rule is the read
split's and does not move: a uid missing from the map, or a chunk that fails, leaves those frames
with no body rather than ending the stream.

## What this is not

**Not a client-facing bodies query.** A `clusterCachedDataObjectsByUID` query belongs to a client
that holds a window of a kind and refetches the rows it holds; this is the same read, made once per
burst instead of once per row, entirely inside the sidecar. The wire does not change.

**Not a fix for the snapshot's size.** The snapshot is still every row, and both sides still hold
the whole kind. What makes a kind of any size work is a paged list plus client-side placement,
which is a larger line of work than this.

## Tests

In `kubestore`, beside the read it adds:

- Known uids come back with their bodies; a uid with no row is absent from the map, not an error.
- An empty uid list makes no query and returns an empty map.
- A body that will not decompress is reported, as `ObjectBody` reports one.

In `clustersvc`, through the existing watch fixture:

- A snapshot of N rows performs one body read, not N — counted through the fixture, which is what
  stops this regressing to per-row the next time the frame builder is touched.
- A burst over the chunk size performs the ceiling of N/chunk reads.
- A row deleted between the diff read and the fetch yields a frame with no body, and the watch
  carries on.

## When it lands

Update the read-split paragraph in `sidecar/CLAUDE.md` — the body fetch is per burst, not per
frame — and note the batch read beside `ObjectBody`. The ADR keeps its reasoning: the split is
unchanged, only how many statements the fetch side takes.
