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
A security spec also has a line in [`TODO.md`](../TODO.md#security) and a row in
[`security-model.md`](../security-model.md); its own *When it lands* section says where the row moves.

## Index

Each spec is self-contained: read one and you can build it. Each header states what it needs and
what it hands on. Both current specs are independent of each other.

| Spec | Scope | Status |
| --- | --- | --- |
| [Connection throughput](connection-throughput.md) | sidecar | Planned |
| [A burst of object frames reads its bodies in one statement](batched-body-reads.md) | sidecar | Planned |
