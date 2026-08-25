---
title: Probe dependencies
scope: sidecar
status: Planned
---

# Probe dependencies

## Goal

Split what `probe.Needs` welded together into the two relationships it actually carries, and make
a probe re-run when the data it reads moves:

- **`WithRequirements(ids...)`** — health. "I cannot run without these."
- **`WithDependencies(ids...)`** — data. "My answer goes stale when these move."

`View` becomes `Observations` on the way through, and every `Run` may read every observation in it.

## The gap this closes

Today's re-arm is derived from *health* transitions only: a probe suspended because what it
needs was failing becomes due when that goes OK. Nothing propagates a **value** change while the
dependency stays OK.

So a connection probe that re-dials a rotated endpoint commits a new `*Connection`, and readiness
— which last succeeded — sits on its 30-second interval using the connection it captured. The
pass runs (every commit asks for one), looks at readiness, and correctly concludes it is not due.
The same hole keeps a probe suspended as `ReasonUnsupported` parked across the very event that
should re-ask it: its doc says "for the life of the connection", and nothing notices a new one.

## Design

**`WithRequirements` is today's `Needs`, renamed.** Untouched before its requirements answer; one
`RequirementFailed` record then suspended for the outage; due again on recovery; re-checked at
dispatch so a worker never spends a timeout learning what the state already said. `needsOf`
becomes `requirementsOf`. Nothing about the lifecycle changes.

**`WithDependencies` is a wake edge and nothing else.** It takes no part in the schedule: `due`
gains no case, and a dependency neither gates a run nor records anything. When a dependency
commits a changed value, the dependent's key goes on `runQ` — the same path `Wake` takes, so it
overrides suspension and a `Skip` parking exactly as a `Wake` does.

**A non-nil commit means the value moved.** The engine holds values as `any` and will not compare
them; the body knows whether its answer changed and says so by returning `nil` when it did not.
This is the contract change every existing body has to absorb, and it reuses the branch already
there:

```go
if val != nil {
	a.value = val
	e.wakeDependentsLocked(k)
}
```

It also settles cleanly against `LastSeen`, where a success with a nil value already means
"confirmed, unchanged" — a run that found the answer the same still dates it.

**`ReasonDependencyFailed` becomes `ReasonRequirementFailed`.** It names a run blocked by the
health edge, so after the split its old name says the opposite of what happened — a reader
hitting the constant would conclude the data edge gated the run. The rename is contained: these
strings never leave `kubeconn` (`clustersvc` derives its own condition vocabulary, and no
GraphQL enum is behind it), so it is a mechanical sweep of `internal/probe`, `kubeconn`'s alias
and tests, and the paragraphs in `sidecar/CLAUDE.md` that lean on the word. The engine's message
becomes "a requirement is failing", naming the probe that is.

**The reverse index is built by `Register`.** A probe declaring `WithDependencies(x)` appends
itself to `dependents[x]` as it registers, so nothing has to walk the specs at wake time and no
freeze step is needed. Both options take IDs `Register` already returned, and `Register` panics
on an ID it has not — dependencies validated exactly as requirements are — so both graphs are
acyclic by construction and a cascade of wakes terminates.

**The two declarations compose without a special case.** A probe declaring both on the connection
gets: the connection commits a new value → wake → dispatch → requirements re-checked → recorded
as `RequirementFailed` if the connection is failing by then, rather than dialed. Each rule fires
in its own place.

**`Observations` replaces `View`**, same slice-backed shape and the same two accessors
(`Attempts(id)`, `Len()`), with the typed read still going through `Handle[T].Get`. Reads are
unrestricted: any `Run` may read any observation, whether or not it declared a relationship to
it. The guard that remains is `Known()` — a probe that has never answered hands back a zero
value, not an error.

**The names carry the whole distinction**, since "requirements" and "dependencies" are near
synonyms in English. Each option's doc comment states which question it answers — *can this run?*
versus *is this answer stale?* — because a reader hitting `WithDependencies(p.connection.ID())`
has nothing else to go on.

## What kubeconn does

The four probes behind reachability declare both on the connection:

```go
p.readiness = probe.Register(e, "readiness", &readinessProbe{conn: p.connection},
	probe.WithInterval(30*time.Second),
	probe.WithRequirements(p.connection.ID()),
	probe.WithDependencies(p.connection.ID()))
```

`connectionProbe.Run` returns `nil` when nothing moved. `connInfo` is comparable (a bool, a
pointer, a string), so that is `if next == prev { ... nil }` — no helper, no reflection.

## Rules

- **A dependency wake never bypasses a requirement.** Requirements are re-checked at dispatch;
  a woken run whose requirements are failing records `RequirementFailed` instead of dialing. So
  a wake costs a record and a publish, never a timeout, and "one timeout per cycle, not one per
  probe" survives the new edge.
- **The data edge does not consult health.** Any recorded commit carrying a changed value wakes,
  whatever its verdict — a failure or a suspension that learned something still learned it, and
  a dependency without a matching requirement must not be gated by health it never declared.
  What keeps this from churning is the nil-when-unchanged contract: a value moves at transitions,
  not every cycle, so a probe that goes on failing commits nothing and wakes nobody.
- **`due` does not learn about dependencies.** They are a commit-time edge; the scheduler's
  cases stay exactly as they are.
- **A wake fires per subject.** A change to subject S's probe wakes S's dependents, nobody
  else's.
- **The value write and the wakes it causes land in one critical section**, so no reader sees
  the new value without the runs it earned already queued.
- **A body that returns a value it did not change costs a run.** Spurious wakes are harmless but
  defeat the interval, which is why the nil-when-unchanged contract is stated on `Run`.

## Build order

Red/green per step; after each: `gofmt -l`, `go vet`, and `go test -race -count=2 ./internal/probe/`.

1. Rename `View` → `Observations` (type, `subject.view()`, the `Run` signature, both kubeconn
   bodies, tests). Mechanical, no behaviour change — land it alone so the diff that follows is
   only semantics.
2. Rename `Needs` → `WithRequirements`, `needsOf` → `requirementsOf`, and
   `ReasonDependencyFailed` → `ReasonRequirementFailed` — the constant, `kubeconn`'s alias and
   its doc, both packages' tests, and the `sidecar/CLAUDE.md` paragraphs that use the word. Also
   mechanical; every existing dependency test keeps its meaning. Landing the reason rename here
   rather than in step 3 is deliberate: for the length of one commit the vocabulary would
   otherwise say the data edge blocked the run.
3. `WithDependencies` and the wake edge: the reverse index in `Register`, `wakeDependentsLocked`
   in `commit`. New tests — a changed value wakes a dependent; an unchanged one (nil commit) does
   not; a wake reaches a suspended dependent; a woken run with a failing requirement records
   `RequirementFailed` rather than dialing; a cascade terminates.
4. `kubeconn`: declare both on the four dependents, and make `connectionProbe` return `nil` when
   nothing moved. Run `go test -race ./internal/clustersvc/...` too.

## Not in this pass

- Gating reads by declaration. Every `Run` sees every observation; the panic-on-undeclared-read
  idea is dropped, since the wake edge removes the reason to read something undeclared.
- Comparing values in the engine. `any` plus `reflect.DeepEqual` on every commit buys nothing the
  body cannot say for free.
- A dependency that carries data into the run directly. Bodies keep reading through their
  captured handles.
- Requirements or dependencies on another *subject's* probes. Both edges are within one subject.

## Done when

- `WithRequirements` and `WithDependencies` are the only dependency options, each documented by
  the question it answers, and no "dependency" survives in the vocabulary of the health edge —
  the recorded reason is `RequirementFailed`.
- A changed value wakes its dependents; an unchanged one does not; `due` is untouched.
- `kubeconn`'s four dependent probes declare both, and the connection probe commits only on a
  change.
- `Observations` is the name throughout, and this spec is deleted — what is then true folded into
  `sidecar/CLAUDE.md`, with the ADR repointed if the dependency story it tells has changed.
