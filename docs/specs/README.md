# Specs

A spec describes **what we are about to build**, in enough detail to implement it and no more.
It is a plan with a shelf life.

The other two doc kinds keep their jobs: `CLAUDE.md` says what is true now, `docs/adr/` says why a
design was chosen. A spec says what comes next.

## Filenames

```
docs/specs/short-slug.md
```

No dates — a spec is edited as the work moves, not appended to.

## Lifecycle

Update the spec while the work is in progress. When it lands, fold what is now true into the
relevant `CLAUDE.md`, write an ADR if a decision needs its reasons recorded, and delete the spec.
A spec left behind after the code ships is a second source of truth.

## Index

| Spec | Scope | Status |
| --- | --- | --- |
| [Cached resource sync](cached-resource-sync.md) | sidecar | Planned |
| [Connection throughput](connection-throughput.md) | sidecar | Planned |
