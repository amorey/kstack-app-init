---
title: Retry resolves with its probe
scope: sidecar + frontend
status: Planned
---

# Retry resolves with its probe

## Goal

`clusterConnectionRetry` resolves when the probe it asked for has finished. The Retry button then
binds to the mutation — `fetching` — instead of inferring its own state from a gauge.

This replaces the client-side inference now in `cluster-sync-panel.tsx` (`useRetryPending`,
`RETRY_START_MS`, the `probing` edge detection). That code is deleted, not extended.

## The problem

The button asks "did *my* run finish?" and the webview cannot answer it. Two facts it needs never
reach it:

- **Which run satisfies the ask.** A `Wake` landing while a run is in flight finds the key held,
  so the ask is redelivered on `Done` and the **next** run satisfies it — never the one already
  out. That is the supervisor's queue semantics, and nothing publishes it.
- **The edges.** `probing` rides `stateHub`, which keeps only the latest value per context. A run
  ending and the requested one starting can arrive as one unbroken `probing`, so the falling edge
  that means "yours is done" is indistinguishable from the one that means "yours has not started".

Every client-side fix is therefore a timing guess, and two review rounds found a defect in each
one: a click during an in-flight probe disarmed the button before the requested run began, and a
coalesced pair left it spinning after the requested run ended. The current 500ms wait is that
guess tuned, not the ambiguity removed.

## Why the sidecar can answer it

`kubeconn` sees **every pass**, not a coalesced level — including the one a run asks for when it
begins. And `Connection.LastAttempt.StartedAt` is a monotonic level: coalescing drops intermediate
values but never regresses one, so a reader comparing it against a baseline is immune to exactly
what defeats edge detection.

That gives an exact test, with no token and no protocol change: **the ask is satisfied by the
first committed attempt whose `StartedAt` is at or after the ask.** A run already in flight
started before the ask, so its commit correctly does not satisfy — which is the supervisor's own
rule, evaluated where it is known.

## Design

### `kubeconn.Service.RetryAndWait(ctx, contextName) error`

Replaces `Retry` at its one call site (`clustersvc.service.RetryConnection`).

1. **Acquire the connection claim**, released on return. Refcounted alongside the one
   `clusterController` holds, as `WatchSchedule` does. A `Wake` on a subject nothing tracks is a
   no-op, and without a claim this would wait for a run that is never dispatched.
2. **Subscribe to the state feed before waking.** A pass landing between the two would otherwise
   be missed — the discipline `WatchSchedule` already states.
3. `askedAt := time.Now()`, then `supervisor.Wake(contextName, probeNames[:]...)` — unchanged, all
   five probes.
4. **Wait** for a `State` whose `Connection.LastAttempt` is done and began at or after `askedAt`.
5. **Bounded**: `ctx`, and a ceiling. On the ceiling, return an error — resolving `true` would
   tell the button a probe finished when none was seen.

**The ceiling is derived, not guessed.** The worst case is a run already in flight burning its
timeout, then the requested one burning its own, plus queue slack: give `registerProbes`'
`10*time.Second` a name (`connectionTimeout`) and express the ceiling as `2*connectionTimeout`
plus slack — 30s. A second literal would let a change to the probe's deadline silently undersize
it. Note what 30s buys: it is also how long the button stays disabled in the worst case, and
urql's three `networkError` retries can stack that to roughly 90s on a flapping IPC socket.

**The connection probe alone decides when this returns**, though the wake reaches all five. That
matches what the gauge reports and what a retry means to a reader: can we reach it.

**Nothing here cancels the run.** The wait's context is the request's; the run is the
supervisor's, and a Wake is not a Restart. A caller that goes away leaves the probe running — do
not "clean up" by cancelling it.

### `clustersvc`

`RetryConnection` passes its `ctx` through to `RetryAndWait` and returns its error. The
`kubeconnSvc` interface and the test fake gain the method. The connectable-record gate is
unchanged.

**Its doc comment goes with it.** Today it says the call "reports only that the record allows a
probe", and that a cluster nothing claims "reaches nothing — the same outcome as asking a cluster
whose probe is already mid-run". Both stop being true: the call now claims the context itself, and
a probe already mid-run is waited through rather than indistinguishable from nothing.

### Schema

`clusterConnectionRetry` keeps `Boolean!`. Only its description changes: it resolves when the
requested probe has finished, rather than when the probe has been asked for. The verdict stays
where it already lives — the record's conditions, streamed by `clustersWatch`.

### Frontend

- Delete `useRetryPending`, `RETRY_START_MS`, and the `wasProbing` edge detection.
- **Move `useMutation(ClusterConnectionRetryMutation)` into `ConnectionDetail`.** The panel-level
  hook is shared by every row, so its `fetching` would spin every open row's button; the hook has
  to be per-row. The `onRetry` prop through `ClusterRow` goes with it.
- `disabled={fetching}`, spinner and "Retrying…" while `fetching`.
- `useNextCheck` and the row's `checking…` are untouched. The two spinners now agree because both
  are driven by the same run, not because their durations were tuned to match.
- Deleting the hook reunites `ConnectionDetail`'s doc comment, which the hook was inserted into
  the middle of.

**A retry can now fail.** The ceiling and a cancelled context come back as operation errors on a
mutation that was infallible in practice. `errorReportExchange` already puts every operation error
on the error bus, so that is the surface — no reporting at the call site, which would show one
failure twice. `fetching` clears on error as it does on success, so the button re-arms either
way.

## Traps

- **A run this call did not ask for can satisfy it, two ways.** A scheduled run beginning in the
  same instant as the wake has `StartedAt >= askedAt` and is taken as the answer. And when
  `RetryAndWait` is the *first* holder of the claim, the entry's own first pass dispatches a run
  before the wake lands — if it begins before `askedAt` the wake then buys a second dial, and if
  after, it answers for the wake. All harmless: each is a fresh dial answering the same question.
  Do not "fix" either by narrowing the match — the alternative is waiting out a probe that has
  already told you what you asked.
- **A run that records nothing does not satisfy it, and the connection probe has two such paths.**
  A `Skip` leaves `LastAttempt` where it was. `connectionProbe.Run` returns one on
  `kubeconfig.ErrNotRead` — an unread kubeconfig names nothing — and `failed` returns one on
  `context.Canceled`, which is a `Restart`, `Remove` or `Close` landing mid-dial. A retry racing
  either waits for the *next* run that records, and the ceiling is what ends it if none comes.
  This is the ceiling's main job, not a theoretical one.
- **urql retries `networkError` up to three times.** A socket dropped mid-probe re-fires the
  mutation, so the sidecar sees a second wake. Acceptable — a wake buys at most one extra dial —
  but do not widen the retry policy to cover this mutation's new duration.
- **No transport timeout may sit below the ceiling.** Neither `invokeFetch` nor the `graphql_query`
  command sets one today; confirm `QueryClient` does not either before choosing the ceiling.

## Costs

One GraphQL request held open per click, for the probe's round-trip and its queue wait — one
goroutine, one state subscription, one claim on the sidecar side. Bounded by the ceiling and by
the button being disabled while it is out.

## Tests

**`kubeconn`** (`service_test.go`) — the attribution rules, which is where they belong:

- Returns once a run that began after the ask has committed.
- **Is not satisfied by the commit of a run already in flight when the ask landed** — the case
  both client-side attempts got wrong. Hold a run open, ask, release it, and assert the call is
  still waiting; then let the next run commit.
- Returns on `ctx` cancellation, and the run is left alone.
- Returns an error at the ceiling when nothing commits.

Channel-based waits throughout (`testutil.Probe`/`Signal`), and the ceiling passed as a parameter
so the test never outwaits the production constant.

**Hold a run open with `cluster.route(path, handler)`** (`kubeconn/testutil_test.go`): a handler
that blocks `/api` on a channel is what puts a real probe in flight. `registerProbes` builds its
own probes, so there is no fake body to inject.

**`clustersvc`** (`service_test.go`): `RetryConnection` forwards the context and returns the
leaf's error; the connectable gate still refuses a record it should.

**Frontend** (`cluster-sync-panel.test.tsx`): the button spins while the mutation is in flight and
clears when it resolves — a deferred `graphql_query` promise in the invoke mock. The existing
"Retry fires clusterConnectionRetry" assertion stands. **Delete the four gauge-driven retry
tests**; nothing on the button reads `probing` any more. The row's own `probing` tests stay.

## Docs to touch when it lands

- **An ADR.** This reverses a deliberate decision — the panel's comment records that the re-probe
  runs out-of-band and the mutation resolves at once. Write `docs/adr/` for why the request and
  its answer became one call, and link it from `sidecar/CLAUDE.md`.
- `sidecar/CLAUDE.md`: the `clusterConnectionRetry` / `Retry` paragraph.
- Delete this spec and its index row.

## Related TODO items

- **Per-probe detail row.** If the requirement ever broadens to *other* windows observing a retry,
  or to the retry's verdict living on the record, that is where a request token stops being
  overhead for one reader and becomes the row's own data. Not needed for this.
- **Run-queue debounce.** The disabled button remains the throttle on a user clicking through an
  outage, and now covers the whole probe rather than a guessed window. Not bundled.
