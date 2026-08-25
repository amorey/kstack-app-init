---
title: Connection probe
scope: sidecar
status: Planned
---

# Connection probe

## Goal

Make `kubeconn` dial. The connection probe stops at resolving the kubeconfig today; after this it
builds a connection, asks the server one question, and classifies the answer:

```go
// today
return probe.Suspend(ReasonResolved, "resolved; nothing dials yet")

// after
return probe.Succeeded()                     // /api answered
return probe.Fail(ReasonUnauthorized, err)   // …or said why not
```

`Lease.Conn` then hands out the connection the probe built, and the four probes behind
reachability become implementable — their dependency is satisfied for the first time.

Out of scope: the four probes themselves. `serverUID` is the next spec, because it writes
`status.server.uid` and that is what `ClusterCache` creation waits on.

## What "connected" means

**Reachable, over TLS we trust, with credentials the server accepts.** One `GET /api`.

`/readyz` belongs to the readiness probe and `/version` to serverVersion, so this probe needs a
request of its own. `/api` is the cheapest one that proves the whole path — DNS, TCP, TLS, then
authentication — and it is the only one of the three that can answer 401 or 403, which is what
gives `ReasonUnauthorized` and `ReasonForbidden` their first producer. Decoding its
`{"versions":[...]}` body is also what tells a Kubernetes API server from a captive portal that
answers 200 to anything.

A 401 or 403 fails the probe rather than succeeding with a caveat: credentials that cannot read
discovery cannot serve the four probes behind this one either, and the failure is the accurate
thing to render.

## Design

### Building a connection

`newConnection(cfg *rest.Config) (*Connection, error)` fills the seam `connection.go` already
declares. Lifted from `internal/kubeconn`'s `build` (`service.go`), which is kept in the tree to
be drawn from:

- Tuning goes on a `rest.CopyConfig` — QPS, Burst, UserAgent are ours to set, and the caller's
  config is not ours to edit.
- `rest.DefaultServerUrlFor`, **not** `rest.DefaultServerURL`: it derives the scheme from whether
  the config carries CA or client-cert data, so a scheme-less plain-HTTP endpoint (a port-forward)
  stays HTTP instead of failing at a handshake. `internal/kubeconn`'s
  `TestEntryForResolvesTheBaseURLWithoutAssumingTLS` comes across with it.
- `rest.HTTPClientFor` once, then `dynamic.NewForConfigAndClient` over that same client — never
  `NewForConfig`, which builds a second pool and undoes the one-socket promise in `Connection`'s
  doc comment.
- `done: make(chan struct{})`.

`keepalive.go` comes across unchanged and `New` calls `configureHTTP2Keepalive()`: it tightens
client-go's HTTP/2 health check from ~45s to ~15s detection, which is how fast anything watching a
cluster notices the connection is gone.
→ [ADR: connection probing](../adr/2026-08-09-connection-probing.md).

### The request

`request.go` holds the one helper every probe here will use — this one for `/api`, serverUID for
`/api/v1/namespaces/kube-system`, principal for the SelfSubjectReview POST. `internal/kubeconn`'s
`do` is the shape, including the two details that are easy to lose: a non-2xx body is drained
(bounded) so an HTTP/1.1 fallback can reuse the connection, and `Accept: application/json` is set.

What changes is the error it returns. A status code is evidence, not a message, so it comes back
typed:

```go
// httpErr is a response that was not 2xx. The code is the whole evidence a raw endpoint leaves,
// so it travels as data rather than inside a formatted string.
type httpErr struct {
	path   string
	code   int
	status string
}
```

### Classification

`classify.go` turns one failed request into the vocabulary `state.go` already declares:

| what came back | Reason |
| --- | --- |
| `*net.DNSError`, ECONNREFUSED, EHOSTUNREACH, other `*net.OpError` | `Unreachable` |
| `x509.UnknownAuthorityError`, `CertificateInvalidError`, `HostnameError`, `tls.CertificateVerificationError` | `TLSInvalid` |
| `context.DeadlineExceeded` | `Timeout` |
| 401 / 403 | `Unauthorized` / `Forbidden` |
| 429 | `Throttled` |
| 500 / 503 | `InternalError` / `ServiceUnavailable` |
| any other status, or a body that will not decode | `Malformed` |

**A `newConnection` error is `ReasonResolveFailed`, not a table entry.** An unparsable host or CA
bytes that will not load fail before a request exists, and the remedy is the same file fix
`ReasonResolveFailed` already names — nothing was dialed. The table covers requests; the build
failure is classified where it happens, in the probe body.

Two things it deliberately does not do:

- **`context.Canceled` is not a Reason.** The caller went away; the run records nothing and
  returns `probe.Skip()`. Only our own deadline is news about the cluster, which is what
  `ReasonTimeout` already says.
- **No `*apierrors.StatusError` branch yet.** `/api` is a raw endpoint, so a status code is all
  there is. The typed branch lands with the first probe that goes through `Dynamic`, per the
  classify-from-the-typed-error rule in `state.go`.

`ReasonUnsupported` gets no producer here — `/api` is not optional. It waits for readiness and
principal, which is where a 404 is ambiguous.

### When a connection is rebuilt

`RESTConfig`'s second return, discarded today, becomes the test for it. `connInfo` gains
`fingerprint string`:

- **unchanged** — keep the connection, and the backoff ladder with it.
- **changed** — build a new one. The old one is retired, and nothing about the old server carries
  over: what the four probes recorded is about credentials that are gone.
- **empty** — the context does not resolve; there is no connection.

The test is *changed fingerprint **or** no connection*, never the fingerprint alone. A build that
fails commits the new fingerprint with a nil connection, so a run keyed on the fingerprint alone
would find it unchanged and never build again — the probe would sit on its backoff ladder retrying
nothing.

Rebuilding wakes the four probes for free, since a moved `connInfo` is what the watch edge already
carries.

### Who retires one

**The probe builds; the service retires.** A run cannot retire the connection it is replacing:
`Commit` is buffered and applied after the run returns, so a probe that closed `done` first would
leave every holder reconnecting against a `Lease.Conn` still handing out the dead one.

`Service.publish` is where it belongs — it runs after the commit, is serialized per context, and
already holds the last-published state to compare against. `entry` gains `conn *Connection`, and
three clauses cover it:

1. **publish, still claimed** — retire `entry.conn` when the snapshot's connection differs, then
   record the new one.
2. **publish, no longer claimed** — retire the *snapshot's* connection. This is not the
   early-return the unclaimed branch is today: a `Release` landing between the commit and the pass
   retires `entry.conn`, which is the **previous** connection, while the one the run just built is
   held by an entry that no longer exists. Returning here is the socket leak the whole rule exists
   to prevent.
3. **`Release`/`Close`** — retire whatever the entry holds when it goes.

Double-retire is why `retire` is `sync.Once`-guarded: clauses 1 and 3 can reach one connection from
either side of the same window, and neither can cheaply prove the other did not.

**One flavor is tolerated, not fixed.** A `Release` racing a run further out removes the subject
before the run returns, and the engine drops a commit against a removed subject
(`Engine.commit` re-checks the `*subject` it captured) — so the connection that run built reaches
neither the snapshot nor the entry, and no clause can name it. The cost is bounded and small: the
`/api` request has already finished, so what is held is idle sockets, which the transport closes at
`IdleConnTimeout` (90s, client-go's inherited default), after which nothing references the client
and it is collectable. Closing that window properly means the engine reporting a refused commit
back to the run, which is a change to a generic engine for one caller's leak — worth doing only if
this shows up as sockets that outlive their contexts.

```go
// retire tells every holder that what it derived over this connection no longer holds, and gives
// the idle sockets back. Once: publish and Release can both reach a connection on the way out.
func (c *Connection) retire() {
	c.once.Do(func() {
		close(c.done)
		c.HTTPClient.CloseIdleConnections()
	})
}
```

### Identity

**`Connection.Identity` is deleted.** It cannot be filled: the three probes that read identity
depend on the connection, so a connection always exists before anything can identify the server
behind it. A field stamped at build time would be empty for the life of every connection.

`State.Identity()` is the read surface in the meantime — same data, fresher. A holder that wants
to know its connection went void watches `Done()`.

The rule that replaces it, when serverUID lands: `connInfo` records the identity the current
connection was validated against, and a **known** part reading differently retires the connection.
A part filling in from empty is not a change — that is the first identification, not a new server,
and treating it as one would rebuild every connection once. The connection probe cannot declare a
watch edge on the identity probes (`resolveLocked` takes only already-registered names, which is
what keeps the graph acyclic), so promptness comes from `publish` calling
`Wake(contextName, nameConnection)` when identity moves. A `Wake` is not an edge; nothing about the acyclicity
guarantee changes.

## API

```go
// connection.go
func newConnection(cfg *rest.Config) (*Connection, error)
func (c *Connection) retire()

// request.go
func (c *Connection) getJSON(ctx context.Context, path string, out any) error
func (c *Connection) postJSON(ctx context.Context, path string, body []byte, out any) error

// classify.go
func classify(err error) (Reason, string)

// probe.go — connInfo gains the fingerprint the connection was built from
type connInfo struct {
	departed    bool
	conn        *Connection
	endpoint    string
	fingerprint string
}
```

`Lease.Conn` keeps its signature and stops blocking on nothing: it answers with the current
connection, or `ErrNoConnection` while there is none. A holder that wants to wait for one pairs
`State` with `WatchState`, as it already does for everything else.

**It hands out a connection whose last probe failed**, which `internal/kubeconn` did not — that
pool waited out failures inside `Conn`. Deliberate: an unchanged fingerprint keeps its connection
across a failing ladder, so a persistent 401 or a control plane mid-restart yields a connection
that is built and currently useless. The holder is the one that can tell those apart, and it
already reads `State.Phase()` and `Connection.Done()` to do it. Withholding the connection would
only move the decision somewhere with less to go on.

The connection probe registers with `probe.WithTimeout(10*time.Second)` — the engine's 30s default
is a whole interval, and a reachability check that takes ten seconds has answered.

## Test seam

**No test may dial a name that leaves the machine.** `fakeKubeconfig` resolves every context to
`https://<key>`, which becomes a real DNS lookup the moment the probe dials — slow, and flaky on a
machine with a wildcard resolver. So the fake serves instead of naming:

- `serving(t)` returns a fake whose contexts resolve to an `httptest.NewTLSServer`, with the
  server's certificate as `CAData`. The handler answers `/api` and is swappable per test.
- A "dead" context resolves to a `127.0.0.1` port with nothing listening — immediate
  ECONNREFUSED, no DNS, no wait.
- Wrong-CA is a second TLS server's certificate against the first server's URL.

`Timeout` and `Canceled` are classified in `classify_test.go` against the sentinel errors
directly. There is no reason to make a test wait out a deadline to prove a switch statement.

Existing tests asserting `ReasonResolved` move to the reason the dial now produces.

## What this deletes

`kubeconn`: `ReasonResolved`, its branch in `State.Phase()`, `Connection.Identity`,
`TestConnReportsThatNothingBuildsOneYet`, and the "nothing dials yet" paragraphs in `probe.go`,
`connection.go` and `service.go`.

`internal/kubeconn` (the old pool) is **not** deleted here. It still holds the material the four
remaining probes draw from — the three request paths, the SelfSubjectReview body, the partial-RBAC
rule. It goes when the last of them lands.

## Build order

1. `newConnection` + `retire`, `keepalive.go` across, `New` calling it. Tests: the base-URL scheme
   cases, and that `Dynamic` and `HTTPClient` share one client.
2. `request.go` and `classify.go` with their tests. Independent of the engine, and the bulk of the
   work.
3. Rewrite `connectionProbe.Run`: resolve → fingerprint → build or reuse → `GET /api` → record.
   Keep the commit-only-on-a-change discipline; it is what stops the four behind it re-running
   every cycle.
4. Retirement in `Service.publish`, `Release`, and `Close`; `entry.conn`; `claim.Conn` answering
   from the snapshot. The release-between-commit-and-publish window gets a test of its own —
   `fakeKubeconfig.duringRead` already exists for interleaving an event with a run in flight, and
   the assertion is that the built connection's `Done` is closed.
5. The test seam, and the existing tests moved off `ReasonResolved`.
6. Fold what is now true into `sidecar/CLAUDE.md`, write the ADR for the endpoint choice and for
   probe-builds/service-retires, and delete this spec.

## Deferred

- **Identity-driven retirement**, as specified above — it lands with `serverUID`, which is the
  first probe that can produce an identity to compare.
- **`*apierrors.StatusError` in `classify`** — with the first probe that goes through `Dynamic`.
- **Debouncing the presence queue** (`TODO.md`) stays deferred. The retry ladder is the engine's
  here, not a third producer on that queue.
