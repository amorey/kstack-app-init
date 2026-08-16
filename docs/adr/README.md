# Architecture Decision Records

An ADR captures **why** a design is the way it is: the forces, the alternatives rejected, and what
the decision costs us. The `CLAUDE.md` files capture **what is true now** — invariants, conventions,
commands, traps. Keeping those jobs apart is the whole point: rationale is read once, current state
is read on every task.

Rule of thumb: if a paragraph in a `CLAUDE.md` explains *why* rather than *what*, or lists an option
we didn't take, it belongs here.

## Filenames

```
docs/adr/YYYY-MM-DD-short-slug.md
```

The date is when the ADR was **written** (for a decision made earlier, that's the day it was written
down — note the approximate original date in the body). Several ADRs can share a date. There is one
flat sequence for the whole repo — no per-subsystem directories, because most decisions here are
contracts *between* subsystems and would otherwise have no natural home. Scope is a frontmatter
field instead, so the index below is still filterable per area.

## Referencing an ADR

Link by path, name it by slug, never by date alone:

```markdown
→ [ADR: single-socket h2c](docs/adr/2026-08-09-single-socket-h2c.md)
```

## Status

- **Accepted** — in force. This is the only status a `CLAUDE.md` may link to.
- **Superseded by [<slug>](<path>)** — kept for the record. Nothing outside `docs/adr/` links here.

Superseding is a normal change, not a failure. Write a new ADR, flip the old one's status, and
**update every `CLAUDE.md` link in the same commit** — a live doc pointing at a superseded ADR is
the one way this system rots.

**A torn-out subsystem's ADRs are deleted, not superseded.** Superseding says "we decided
differently"; a teardown ahead of a rebuild has decided nothing yet, and a Superseded entry with no
successor to point at is worse than no entry. Remove the files, their index rows, and every inbound
link in the same commit — git history keeps them — and write the replacements as the new design
lands. Superseding remains the rule everywhere else.

## Exemption from the "describe the present" rule

The repo rule (root `CLAUDE.md`) is that code comments and docs describe only the current state —
no "used to", "formerly", "superseded". **`docs/adr/` is the sole exemption.** ADRs are an
append-only historical log; recording what we rejected, what we replaced, and what we believed at
the time is their function. Edit an accepted ADR only to fix errors or flip its status — do not
rewrite its decision to match later reality. Write a new one.

## Writing one

Copy [`TEMPLATE.md`](TEMPLATE.md). Keep it to a page. Prose over bullets where the reasoning is a
chain; the "Alternatives considered" section is not optional — an ADR with no rejected option is
usually just documentation in the wrong place.

## Index

| Date | ADR | Scope | Status |
| --- | --- | --- | --- |
| 2026-08-09 | [Record architecture decisions](2026-08-09-record-architecture-decisions.md) | repo | Accepted |
| 2026-08-09 | [Multiplex GraphQL and gRPC over one socket with h2c](2026-08-09-single-socket-h2c.md) | cross-cutting | Accepted |
| 2026-08-09 | [Route all webview GraphQL through Tauri IPC](2026-08-09-graphql-over-tauri-ipc.md) | cross-cutting | Accepted |
| 2026-08-09 | [Stream each kind as its own delta watch, joined client-side](2026-08-09-delta-watch-protocol.md) | cross-cutting | Accepted |
| 2026-08-09 | [Transport status keyed to the host's open frame](2026-08-09-transport-status-generation.md) | frontend | Accepted |
| 2026-08-09 | [host.json as settings source of truth](2026-08-09-host-json-settings.md) | cross-cutting | Accepted |
| 2026-08-09 | [First-paint theming: inline script + native background](2026-08-09-first-paint-theming.md) | cross-cutting | Accepted |
| 2026-08-09 | [Per-platform window chrome](2026-08-09-per-platform-window-chrome.md) | host | Accepted |
| 2026-08-09 | [URL search params as window state](2026-08-09-url-params-as-window-state.md) | frontend | Accepted |
| 2026-08-09 | [Dashboard nav: curated tree merged with discovered kinds](2026-08-09-dashboard-nav-merge.md) | frontend | Accepted |
| 2026-08-09 | [Beehive owner chain with ObjectID identity](2026-08-09-beehive-control-plane.md) | sidecar | Accepted |
| 2026-08-09 | [Every condition is a liveness condition](2026-08-09-liveness-conditions.md) | sidecar | Accepted |
| 2026-08-09 | [Local-first auth; settings sync depends on auth](2026-08-09-local-first-auth-settings.md) | sidecar | Accepted |
| 2026-08-09 | [Resync via fan-out poke, not cascade](2026-08-09-poke-resync-fanout.md) | cross-cutting | Accepted |
| 2026-08-09 | [Per-cluster parallel probing, sentinel, backoff-neutral retries](2026-08-09-connection-probing.md) | sidecar | Accepted |
| 2026-08-09 | [Sandbox build-output isolation](2026-08-09-sandbox-build-separation.md) | repo | Accepted |
| 2026-08-10 | [ClusterService as record-family sub-APIs](2026-08-10-cluster-service-sub-apis.md) | sidecar | Accepted |
| 2026-08-10 | [Split internal/cluster into boundary, controllers, and domain](2026-08-10-cluster-package-split.md) | sidecar | Accepted |
| 2026-08-14 | [Report a dead watch as a terminal GraphQL error](2026-08-14-watch-failure-reporting.md) | cross-cutting | Accepted |
| 2026-08-16 | [Compose start/stop/close as one shape](2026-08-16-lifecycle-composition.md) | sidecar | Accepted |
