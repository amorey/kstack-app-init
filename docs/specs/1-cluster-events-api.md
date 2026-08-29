---
title: Cluster events API
scope: sidecar
status: Planned
order: 1
---

# Cluster events API

**Needs:** nothing. **Hands on:** nothing. First because it is the only gap that is a live
failure — the webview opens `eventsWatch` on the sync panel today and gets
`internal system error` back.

## Goal

Serve the control-plane event timeline that four schema fields already declare.

```go
// internal/clustersvc/service.go
func (s *service) ListEvents(ctx context.Context, id ObjectID, category *string, limit *int) ([]Event, error) {
	panic("not implemented")
}

func (s *service) WatchEvents(ctx context.Context, id ObjectID, category *string) (*Stream[EventWatchFrame], error) {
	panic("not implemented")
}
```

These two panics are the only ones left in `clustersvc`. They take down
`Cluster.events`, `ClusterCache.events`, `ClusterCachedKind.events` (all three resolvers call
`ListEvents`) and the `eventsWatch` subscription. gqlgen recovers the panic into the generic
`internal system error`, which is why the log line names no cause.

Only the read side is missing. The writes are already landing: `caches.go:1011` and
`cachedkinds.go:399` both call `client.AddEvent`, under the categories `connection`,
`discovery` and `sync`.

## What beehive already supplies

```go
ListEvents(ctx, id ObjectID, opts ...EventOption) ([]Event, error)
WatchEvents(ctx, id ObjectID, opts ...EventOption) (*EventStream, error)
```

Three properties decide the shape of the implementation:

- **Both read by id and are not kind-scoped** — "an id names one row whatever its kind". So
  clusters, caches and kinds are served by *one* client and one code path, not a per-kind switch.
- **`WatchEvents` requires a registered controller for the calling client's kind**, "a property
  of the caller and not of the target". All three kinds register one, so any of them works; pick
  `s.clusterClient` and say why in a comment, or the next reader will assume the choice was
  arbitrary and switch it to the target's kind.
- **`WithEventCategory` and `WithEventLimit` cover both parameters**, but neither maps by
  passing the argument through. See the two conversions below.

`EventStream` is the snapshot plus the tail:

```go
type EventStream struct {
	Runs            []Event         // snapshot, newest-first
	ResourceVersion int64           // the position Runs was read at
	Events          <-chan Event    // runs above it, oldest-first, closed exactly once
	Retention       EventRetention
}
func (s *EventStream) Err() error
```

## Design

### `ListEvents`

A projection over `beehive.Event`, the same shape `toCluster` and friends have. `Event.ID` is a
`beehive.EventID`; the served `clustersvc.Event.ID` is an `ObjectID` — see the id caveat below.

**A nil `category` must add no option at all.** `WithEventCategory("")` selects the *default*
timeline, which beehive documents as distinct from no filter — and every write in this package
carries `connection`, `discovery` or `sync`, so passing the empty string returns nothing rather
than everything. Only `nil → no option` is correct:

```go
var opts []beehive.EventOption
if category != nil {
	opts = append(opts, beehive.WithEventCategory(*category))
}
if limit != nil {
	opts = append(opts, beehive.WithEventLimit(*limit))
}
```

**A nil `limit` is deliberately unbounded**, and that is safe here rather than an oversight:
`beehive.New` is built with `WithEventRetention(maxEventRuns, 0)` and `maxEventRuns` is 20
(`service.go:384`), so retention already caps each `(object, category)` timeline. An unfiltered
read is 20 × the number of categories that object writes — at most three. Say so where the option
is built, so the missing bound reads as a decision.

### `WatchEvents`

`NewStream` with a pump: snapshot as `Run` frames, one `Bookmark`, then the tail. Newest-first is
the snapshot's order and the client upserts by id, so the pump forwards `Runs` as beehive gives
them rather than reversing.

The `beehive.WatchEvents` call is **synchronous, before `NewStream`**, so a subscribe failure
returns to the resolver as an error instead of becoming a terminal frame on a stream the client
already has.

```go
src, err := s.clusterClient.WatchEvents(ctx, beehive.ObjectID(id), opts...)
if err != nil {
	return nil, err
}
return NewStream(ctx, func(ctx context.Context, out chan<- EventWatchFrame) error {
	for _, ev := range src.Runs {
		if !sendFrame(ctx, out, EventWatchFrame{Type: EventFrameRun, Event: toEvent(ev)}) {
			return nil
		}
	}
	if !sendFrame(ctx, out, EventWatchFrame{Type: EventFrameBookmark}) {
		return nil
	}
	for ev := range src.Events {
		if !sendFrame(ctx, out, EventWatchFrame{Type: EventFrameRun, Event: toEvent(ev)}) {
			return nil
		}
	}
	return terminalErr(src.Err())
})
```

This is `deltaWatch.pump` with two frame types instead of four and no removal case. **Do not
generalize `deltaWatch` to cover it**: the shapes agree only in the bookmark, and the parameter
that would unify them (a removal that cannot occur) is exactly the distinction `EventFrameType`
exists to make.

### A collected record is a clean end, not a failure

`Stream.Err` is what turns a dead watch into the `watchFailed` extension rather than a silent
reconnect loop, and the resolver already routes it (`watchStream` in `graph/watch_failure.go`).
Forwarding `src.Err()` unfiltered therefore ships a bug:

```go
// beehive/eventswatch.go
func isTerminalWatchErr(err error) bool {
	return errors.Is(err, ErrWatchTooOld) || errors.Is(err, ErrNotFound)
}
```

`ListSince` answers `ErrNotFound` "when id holds no object: its log cascaded away with it", and
the reader files that as terminal. So **a record collected while its timeline watch is open ends
the stream with `ErrNotFound`**, and the user gets an error. That is a reachable path, not a
corner: the sync panel subscribes on a `ClusterCachedKind` id read out of a live watch
(`cluster-sync-panel.tsx:770`), and clearing the cache deletes those records.

It is also the case `stream.go:33` already legislates for the cached-data watches — "a user
pressing a button is not a broken watch, and reporting one would put an error in front of them
per open watch." Same answer here:

```go
// terminalErr keeps the reasons a consumer can act on. A collected record is not one:
// its log cascades away with it, so the watch ending is the deletion arriving, and
// reporting it puts an error in front of a user who pressed the button.
func terminalErr(err error) error {
	if errors.Is(err, beehive.ErrNotFound) {
		return nil
	}
	return err
}
```

`ErrWatchTooOld` stays reported: retention passing the tail's cursor means runs were lost, and a
resubscribe is what makes the client correct again.

**The asymmetry is worth knowing before anyone tests it.** An id that has *never* held a row does
not fail — `checkExists` leaves `resolved` unset and the drain no-ops, so the watch bookmarks an
empty snapshot and waits (the "opened ahead of its object" case beehive supports deliberately).
Only a record that was there and went produces the error. Testing a bogus id proves nothing about
this path.

The two halves meet on the client: a clean end still reconnects (`subscribe-exchange.ts:171`
treats a graceful `complete` as "reconnect silently"), so the webview re-subscribes on the
collected id and lands in the never-existed branch — one idle subscription sitting on an empty
bookmarked snapshot, dropped as soon as `timelineKind?.id` moves. That is what "quietly" means
here; it is not a reconnect loop.

## The id caveat — decide it in this pass

`clustersvc.Event.ID` is typed `ObjectID`, and the `ObjectID` scalar's own schema doc says ids
"are unique across kinds, not just within one, so they never collide". Event runs come from
beehive's separate `EventID` sequence, so an event id can numerically equal an object id and that
sentence stops being true the moment this ships.

Nothing breaks in practice — the client uses `event.id` only as an upsert key within one
timeline — but the type is claiming something it no longer holds. Two honest options:

- A separate `EventID` scalar bound to a distinct Go type. Correct, and costs a scalar plus the
  generated bindings.
- Keep `ObjectID` and narrow the scalar's uniqueness sentence to objects, saying explicitly that
  an event id is unique within a timeline only.

**The second is the cheaper consistent choice**, and the schema has already half-hedged toward
it: `Event.id` is documented as "an opaque int64 id, same wire form as ObjectID" — describing the
encoding rather than claiming the identity. Narrowing the scalar's sentence makes the two agree.
Take the first only if something is going to hand an event id where an object id is expected.

Leaving the doc as-is is the one outcome to avoid.

## Rules

- **One implementation for all three kinds.** The read is by id; a per-kind branch would be
  three copies of the same call.
- **The bookmark closes the snapshot, always** — including for an empty timeline, which is the
  case that tells "no events" apart from "still loading".
- **`Err` is forwarded, except `ErrNotFound`.** A watch whose source died must report it; a
  record that was collected is a clean end, because the deletion is the answer.
- **No filtering in this package.** Category and limit are beehive options.

## The harness step 2 needs, which does not exist yet

Step 1 is clean — `ListEvents` does not check registration, and the existing `newTestDeps` covers
it. Step 2 needs three things the package has never needed together.

**A beehive that is both running and registered.** `clientImpl.WatchEvents` refuses a kind with no
controller, and this is the first watch in the package to require that. The two harnesses each
supply half: `newTestDepsAndBeehive` registers controllers but never starts beehive, and
`newRunningBeehive` starts it while deliberately registering none — "a reconcile writing status
would put frames on a watch that the test never asked for" (`testutil_test.go:394`). A live tail
is what notices the deletion, so step 2 needs both, and the harness is new work.

**Then live with the reason that comment exists.** Registered and running means the reconcilers
write their own runs into the timelines under test. Scope the assertions to a category no
controller writes, or assert on the runs the test put there rather than on the whole snapshot.

**Two cadences to shrink, not one.**

- `WithGCInterval` — the record has to actually be collected. `caches_test.go:253` is the
  pattern (`newRunningDeps(t, beehive.WithGCInterval(time.Millisecond))`).
- `WithWatchFloorInterval` — and this is the one that gets missed. Only an event *write* wakes the
  reader: `signalEventsWritten` is the sole sender on `eventWriteHub`, and collection never calls
  it. So the reader learns its row vanished on its floor tick, which defaults to 30s. Left alone
  the test outwaits a production constant, which the repo's testing rules forbid outright — and
  the tempting wrong answer is a `time.Sleep`.

Both are `beehive.Option`s, and `newTestBeehive` already takes `opts ...beehive.Option` for
exactly this. The work is a constructor that registers, starts, and forwards them.

## Build order

1. `ListEvents` + `toEvent`, against the three kinds. One test per kind is enough to pin that the
   read is not kind-scoped.
2. `WatchEvents` and the pump: snapshot → bookmark → tail; an empty timeline still bookmarks; a
   failed source surfaces through `Stream.Err`. Pin the two endings separately — **delete the
   watched record and assert `Err` is nil**, and assert an ordinary failure still reports. Nobody
   writes that `errors.Is` from "Err is forwarded", and without the first test the bug ships.
   Build the harness above first; the deletion test cannot run on either existing one.
3. The id decision above, with its schema edit.

## Not in this pass

- **Retention surfaced to the client.** `EventStream.Retention` tells a consumer how to bound its
  own list; the schema already warns that the accumulated set can outgrow the server's. Worth
  serving when a client actually keeps a long timeline, not before.
- **`WithEventsResumeFrom`.** The webview reconnects with a fresh snapshot, and
  `useWatchSubscription` discards the previous generation's accumulator anyway.
- **A whole-fleet event feed.** Every read here is one object's log.

## Done when

The sync panel's `ClusterSyncEvents` subscription renders runs instead of logging
`internal system error`, a cluster with no events shows an empty timeline rather than a spinner,
`clusterCachedKind { events(category: "sync") }` answers, and clearing a cache with the panel
open closes its kind timelines quietly instead of raising an error per open watch.

`sidecar/CLAUDE.md`'s event section gains the served shape in the same commits. Delete this spec
when step 3 lands.
