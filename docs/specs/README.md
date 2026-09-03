# Specs

A spec describes **what we are about to build**, in enough detail to implement it and no more.
It is a plan with a shelf life.

The other two doc kinds keep their jobs: `CLAUDE.md` says what is true now, `docs/adr/` says why a
design was chosen. A spec says what comes next.

## Filenames

```
docs/specs/<n>-short-slug.md   a spec with a place in a sequence
docs/specs/short-slug.md       one that stands alone
```

No dates — a spec is edited as the work moves, not appended to. **The number is the build order**,
so the directory listing is the plan; a spec with no place in a sequence goes without one.
Renumber when the plan changes, and fix the links in the same edit — the numbers are load-bearing
only as far as they agree with each spec's own header.

## Lifecycle

Update the spec while the work is in progress. When it lands, fold what is now true into the
relevant `CLAUDE.md`, write an ADR if a decision needs its reasons recorded, and delete the spec.
A spec left behind after the code ships is a second source of truth.
A numbered security spec also has a line in [`TODO.md`](../TODO.md#security) and a row in
[`security-model.md`](../security-model.md); its own *When it lands* section says where the row moves.

## Index

Each spec is self-contained: read one and you can build it. Each header states what it needs and
what it hands on — a numbered spec that needs nothing is ordered by priority rather than by
dependency, so it can be built out of turn at a cost you can read off its header.

**Independent.**

| Spec | Scope | Status |
| --- | --- | --- |
| [Connection throughput](connection-throughput.md) | sidecar | Planned |
| [A burst of object frames reads its bodies in one statement](batched-body-reads.md) | sidecar | Planned |

**Security hardening**, in build order — cheapest and most enforceable first, decisions last. Each
one stands alone; the numbers are a recommended sequence, not a dependency chain, except where a
spec's header says otherwise. They come from
[`security/2026-09-02-threat-model.md`](../security/2026-09-02-threat-model.md).

| Spec | Scope | Status |
| --- | --- | --- |
| [7 · Endpoints come from arguments, not environment](7-endpoints-as-arguments.md) | host · sidecar · build · ci | Planned |
| [8 · Updates say what they actually are](8-updates-say-what-they-are.md) | host · docs · ci | Planned |
| [9 · Cached events age out](9-bound-the-events-table.md) | sidecar | Planned |
| [10 · A kubeconfig exec plugin waits for approval](10-approve-exec-credential-plugins.md) | sidecar · frontend | Planned |
| [11 · The host sends only the operations the app ships](11-allowlist-graphql-operations.md) | host · frontend · build | Planned |
| [12 · Decide what the cluster cache is](12-decide-what-the-cache-is.md) | sidecar · decision | Planned |
| [13 · A cache stops growing before the disk fills](13-a-cache-size-ceiling.md) | sidecar | Planned |
