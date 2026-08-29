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

## Index

Each spec is self-contained: read one and you can build it. Each header states what it needs and
what it hands on — a numbered spec that needs nothing is ordered by priority rather than by
dependency, so it can be built out of turn at a cost you can read off its header.

**Sequenced — the build order.** Spec 3 is the last gap between this branch and `main`'s cluster
service; 4 is a cost repair on top. Neither depends on the other; the order is what to do first.

| Spec | Scope | Status |
| --- | --- | --- |
| [3 — Per-kind sync pause](3-per-kind-sync-pause.md) | sidecar, frontend | Planned |
| [4 — Object read split](4-object-read-split.md) | sidecar | Planned |

**Independent.**

| Spec | Scope | Status |
| --- | --- | --- |
| [Connection throughput](connection-throughput.md) | sidecar | Planned |
