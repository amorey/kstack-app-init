---
title: Identity-driven connection retirement
scope: sidecar
status: Planned
---

# Identity-driven connection retirement

## Goal

Turn a recorded identity conflict into a rebuilt connection.

The detection half shipped with [ADR: connection-carried identity](../adr/2026-08-25-connection-carried-identity.md):
a `Connection` carries a set-once `serverUID`, a second, different UID read over it records a
conflict (`setServerUID`), and `ConnFor` then refuses every caller. That made a server replaced
behind unchanged credentials a **visible stall instead of silent corruption** — and left the stall
permanent, because `connectionProbe.Run` rebuilds only on a changed credential fingerprint or a
missing connection, and this failure mode changes neither. This spec is the recovery half: the
conflict rebuilds the connection, and the stall becomes a window.

Everything below is `internal/clustersvc/internal/kubeconn`. No schema, GraphQL, or `kubecatalog`
change — the downstream recovery is existing wiring doing its job (see the sequence).

## Design

Two changes, both small.

### 1. The rebuild condition learns about the conflict

`connectionProbe.Run` currently rebuilds when `next.conn == nil || next.fingerprint !=
fingerprint`. Add the third arm: the standing connection answers as conflicted. `Connection` grows
an unexported `conflicted() bool` (one `identity.Load()`; the probe is in the same package), since
`ServerUID()`'s `("", false)` deliberately cannot tell conflicted from not-yet-identified and the
rebuild must fire only for the former.

- **The trigger is the conflict on the connection, never a comparison against
  `State.Identity()`.** The identity observable lags a rebuilt connection by a dispatch plus a
  round-trip (the stale pairing the ADR exists for); the conflict was recorded by the probe that
  made the request over this connection, so there is one writer and nothing to correlate.
- **First identification rebuilds nothing.** A stamp filling in from empty records no conflict —
  `setServerUID` already makes that distinction — so the "a part filling in is not a new server"
  rule needs no guard here.
- The rebuild path is the existing one, unchanged: the new connection lands in a committed
  `connInfo` (new pointer, so the comparable value moves), the four watching probes re-run,
  `publish`'s `record` retires the conflicted connection nothing holds, and its `Done()` tells
  holders. A build or dial that then fails follows the ordinary ladder; the deferred commit
  already guarantees the conflicted connection is replaced-and-retired even on that path.

### 2. Promptness: `publish` wakes the connection probe when the conflict lands

Without a wake the rebuild waits out the connection probe's 30s interval — change 1 alone already
recovers within that. The probe cannot declare a watch edge on the serverUID probe —
`resolveLocked` takes only already-registered names, which is what keeps the probe graph acyclic —
so the wake rides `publish`, the `OnPass` that runs after the serverUID pass that recorded the
conflict. It goes inside the existing `if changed` block, after the `!held` return, so a released
context is never woken:

```go
conn := keyConnection.From(v).Value.conn // already in hand for record
...
if changed {
    s.signalHub.Sender().Send(contextName, struct{}{})
    if conn != nil && conn.conflicted() {
        s.engine.Wake(contextName, nameConnection)
    }
}
```

- **A `Wake` is not a dependency edge**: it adds nothing to the probe graph, and `publish` is
  called after `pass` releases the engine's lock (`engine.go`, the unlock before `e.onPass`), so
  waking from it cannot deadlock.
- **Gated on `changed` — an edge, deliberately.** A `Wake` is a queue add, not a schedule, so
  nothing paces a condition that outlives the run meant to clear it. Wake on the level and one
  reachable path spins: a conflicted connection whose kubeconfig stops resolving (a deleted CA
  file, a broken exec plugin) returns `ReasonResolveFailed` *before* the rebuild branch, the
  deferred commit sees nothing moved, and every finished run re-queues its subject's pass
  (`passQ.Add`) — publish → conflicted → Wake → run → fail → publish, an unbounded hot loop that
  bypasses the backoff ladder, rebuilds `RESTConfig`'s TLS material each turn, and floods every
  `stateHub` watcher out to the webview.
- The edge exists for free: recording the conflict moves `news.vouchedFor` from the uid to empty,
  so `changed` is true on exactly the pass that records it. On the spin path the wakes bound at
  two — the conflict, then the first `ok[connection]` flip — after which failed runs move no news
  and retry on the ladder alone.
- **The interval is the backstop, not the wake.** The rebuild arm in change 1 checks the
  connection on every run, so a lost edge costs at most 30s of promptness, never the recovery.

## The sequence after the wake

What recovery looks like end to end — every step below the first is wiring that already exists:

1. The connection probe rebuilds over the unchanged fingerprint and commits; the conflicted
   connection is retired, `Done()` fires for holders.
2. The commit re-runs the four probes watching it; `serverUIDProbe` reads the replacement server
   and stamps the new connection.
3. `news.vouchedFor` refills, `signalHub` fires, and kubecatalog's bridge wakes the subjects over
   the context.
4. A subject armed for the **old** identity is refused (`IdentityMismatch`) and suspends — correct,
   its cache is not this server; the cluster pass folds the new UID into `status.server.uid`, and
   the reconcile chain arms a subject for the new identity, whose sweep runs over the new
   connection and stands its watcher up.

`Retry`/`clusterConnectionRetry` also becomes a working manual recovery for a conflicted cluster:
it wakes all five probes, and the woken connection run now hits the rebuild arm. Today that button
does nothing for this state.

## Rules

- **The stamp is still never overwritten.** Retirement replaces the connection; it never launders
  a conflicted one back into service.
- **Never correlate a connection with `State.ServerUID`** — unchanged from the ADR; this spec adds
  no second reader of the pairing.
- An endpoint flapping between two servers re-records a conflict per flap and rebuilds per flap,
  paced by the serverUID probe's cadence. Acceptable, and pathological enough not to design for.
- **`Conn` still hands out a conflicted connection** until the rebuild lands — pre-existing
  (`Done` is how a holder hears about retirement, and `ConnFor` is the identity-scoped gate),
  unchanged here. This spec shortens how long one stands; it does not close that window.

## Testing

- `connection_test.go`: `conflicted()` — false for nil identity, false for a stamped one, true
  after a second, different UID.
- `probe_test.go`: a `connInfo` whose connection is conflicted and whose fingerprint is unchanged
  → `Run` builds a new connection and commits it; the first stamp alone does not rebuild.
- `service_test.go` (kubeconn, end to end): stamp uid-A, have the serverUID probe read uid-B →
  the connection probe is woken, the old connection's `Done()` fires, and the fleet signal
  eventually carries `vouchedFor` = uid-B.
- `service_test.go`, the spin path: a conflicted connection whose `RESTConfig` fails → the wakes
  are bounded (none beyond the two news edges), so a broken kubeconfig can never hot-loop the
  probe. A negative assertion, so it takes a bounded window sized off the probes' (shrunk)
  cadence.
- `kubecatalog/service_test.go` (the acceptance condition): after a conflict-driven refusal stops
  the watcher, a pool that hands out an identified connection again gets the sweep re-run **and
  the watcher standing over the new connection** — `ensureWatcher`'s connection comparison doing
  its job, nothing new. The recovery is not done when the sweep runs; it is done when the watch
  stands.

The usual rules: no magic sleeps — `testutil.Probe`/`Signal`/`Recv`, and the probes' seams.

## Not in this pass

- **Surfacing the conflict on the record.** While the rebuild is in flight the stall is
  sub-minute; the per-probe observability row (`TODO.md`) is where a lingering one would show.
- Retirement for any other reason (staleness, error budgets). The conflict is the only identity
  fact with one writer; everything else stays on the fingerprint.

## Done when

The kubecatalog acceptance test passes; the `TODO.md` item is deleted; the "specified and
unbuilt" paragraphs in `sidecar/CLAUDE.md` and the `→ TODO.md` pointer in `setServerUID`'s
comment are rewritten to describe the built behavior; a short ADR records rebuild-on-conflict and the
edge-gated wake (ADR 2026-08-25 stays Accepted — it anticipated this half); this spec is deleted.
