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
