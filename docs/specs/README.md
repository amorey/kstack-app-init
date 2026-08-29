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

**Sequenced — the build order.** Specs 5-7 are one line of work: the SQLite techniques beehive
uses that `kubestore` does not. Each states what it needs; 5 makes the last varying statement's
text constant, and 6 caches what is then cacheable. 7 needs nothing and touches only the schema,
so it is ordered by priority. The open contract 1-3 settled has landed, as has 4's `json_each`
idiom.

| Spec | Scope | Status |
| --- | --- | --- |
| [5 — A delete returns the uids it removed](5-deletes-return-their-uids.md) | sidecar | Planned |
| [6 — Prepare each statement once, not once per call](6-prepared-statement-cache.md) | sidecar | Planned |
| [7 — The all-key tables lose their rowid](7-without-rowid-key-tables.md) | sidecar | Planned |

**Independent.**

| Spec | Scope | Status |
| --- | --- | --- |
| [Connection throughput](connection-throughput.md) | sidecar | Planned |
