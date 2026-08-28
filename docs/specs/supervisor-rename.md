---
title: The probe engine becomes the supervisor
scope: sidecar
status: Planned
needs: nothing
hands to: The mirror on the supervisor (today `kubesync-mirror-on-probe-engine.md`, renamed here)
---

# The probe engine becomes the supervisor

## Goal

Rename `internal/probe` to say what it does. The package keeps a set of subjects each with a
standing value, re-runs a body on an interval, climbs a backoff ladder when it fails, parks it
until a dependency moves, and hands back what it stops holding. That is a supervisor; "probe" is
what its first bodies happened to do. The mismatch is small while every body fetches an
observation and stops being small once one of them establishes a stream
([the mirror](kubesync-mirror-on-probe-engine.md)), so the rename lands first.

Nothing behaves differently after this spec. It is one commit of names, done before the mirror
spec's step 1 so the mirror is written in the new vocabulary rather than renamed under it.

## The names

| Today | After | Why |
| --- | --- | --- |
| package `internal/probe` | `internal/supervisor` | what the package does, not what one body does |
| `Engine` | `Supervisor` | the package's one noun; `New` still builds it |
| `Probe[T]` | `Reconciler[T]` | a body reconciles its subject's value with what it finds — a fetch and a stream alike |
| `Probe.Run` | `Reconciler.Reconcile` | the controller-runtime spelling, for the audience this code has |
| `ProbeOption` | `ReconcilerOption` | pairs with `Option`, which stays the `Supervisor`'s |
| `probeID`, `probeCfg` | `reconcilerID`, `reconcilerCfg` | unexported, follow the type |
| `Engine.probes`, `runProbe` | `reconcilers`, `runReconciler` | unexported, follow the type |
| `probeFunc` (test adapter), the seven `Test…Probe…` names | `reconcilerFunc`, `Test…Reconciler…` | the tests name the body too |

Everything else is already named for the supervisor's job and stays: `Register`, `Add`, `Remove`,
`Wake`, `WakeAll`, `Read`, `OnPass`, `Start`, `Close`; `Pass` and its accessors; `Result` with
`Succeeded`/`Fail`/`Suspend`/`Skip` and `RequeueAfter`; `Reason`, `Verdict`; `Attempt`,
`Attempts`; `Snapshot`, `Observation`, `Get`, `Key`; `Backoff`; every `With*` option. The panic
prefix follows the package: `supervisor: no reconciler named …`.

**"Run" stays as the noun for one call of `Reconcile`.** "A run's `Result` is its schedule", "a
run in flight", `runQ`, `runLoop` — all keep. The method is what a body implements; a run is one
execution of it. Renaming the noun would touch every sentence in the engine's docs for no gain in
precision.

**"Probe" stays where a body actually probes.** `kubeconn`'s five probes read a server —
`probe.go`, `registerProbes`, `connectionProbe`, `serverUIDProbe`, `probeNames`, the ADRs about
them, and `probing` on the wire (`clusterScheduleWatch`) — and `kubesync`'s sweep is three probes
over the discovery documents (`probeAPIVersions`, `probeAPIGroups`, `probeResources`). Those are
domain names for bodies that observe, and they are correct. So is `testutil.Probe[T]`, the
channel tripwire the tests wait on — a third meaning, unrelated, untouched. What changes is only
the sentence "a probe is a struct implementing `probe.Probe[T]`", which becomes "a probe is a
reconciler whose value is an observation".

**"Engine" changes only where it means this package.** `prefsync.Engine` is another thing with the
same name and keeps it, in code and in its `sidecar/CLAUDE.md` and `TODO.md` mentions. And
`sidecar/CLAUDE.md`'s heading *The sync engine (`internal/clustersvc/internal/kubesync`)* names
the subsystem, not the machinery under it — it stays.

**Fields holding one** are named for what they supervise, in full: `kubeconn.Service.engine` →
`supervisor`; `kubesync.Service.discoveryEngine` → `discoverySupervisor`, and the mirror spec's
`mirrorEngine` → `mirrorSupervisor`.

## Prose

The same substitution runs through the comments and docs, with one rule: **"engine" → "supervisor"
everywhere, "probe" → "reconciler" only where the word means the registered body.** A sentence
about what the connection probe found is about a probe.

- `internal/supervisor` package and type comments. The package doc's first sentence becomes: *Package
  supervisor keeps a set of subjects each holding a value, and runs the reconcilers registered over
  them: the controller pattern without Kubernetes — a work queue, a level-triggered pass, and a
  schedule derived from recorded state.*
- `sidecar/CLAUDE.md`: the `internal/` layout bullet; the section *The probes (`probe.go`) over the
  engine (`internal/probe`)* — which the file currently carries **twice**, verbatim; keep one —
  and the kubesync section's mentions of the engine. The connection-pool prose about the five
  probes is untouched. That section's link reads `→ [ADR: probe engine](…)`: the link text becomes
  `→ [ADR: the supervisor's extraction](…)` and the ADR's own title stays, as every ADR's does.
- `docs/specs/kubesync-seam.md`, the engine mentions in the shape and build-order sections.
  `connection-throughput.md` names only the connection probe and is untouched.
- `docs/specs/kubesync-mirror-on-probe-engine.md` → `kubesync-mirror-on-supervisor.md`, retitled,
  its `mirrorEngine` and "probe engine" wording moved, its decision 5 deleted. Fix the links in
  `docs/specs/README.md` and `kubesync-seam.md` in the same edit, and drop this spec's own row from
  the README when this file goes.
- `docs/adr/`: untouched, as always. One new ADR records the rename and the reasoning — the table
  above against controller-runtime's vocabulary, and why "supervisor" over "reconcile" for the
  package (the package is the thing that keeps reconcilers running; a reconciler is what a caller
  writes). The 2026-08-24 extraction ADR stays Accepted: the decision it records stands, only the
  name moved. Add the row to `docs/adr/README.md`.

## Mechanics

1. `gopls rename` every symbol in the table, in its order — a whole-program rename the tool does
   correctly and `sed` does not: `prefsync.Engine.Run` and `poke.Service.Run` are unrelated
   methods that must not move. `gopls` is not in the sandbox; `go install
   golang.org/x/tools/gopls@latest` needs the network, so it is a prerequisite, not a fallback.
2. `git mv sidecar/internal/probe sidecar/internal/supervisor`; rewrite the package clause and the
   import path; the local alias every importer gets is `supervisor`, so no import is aliased.
3. The prose pass, file by file, by hand. Four greps define done:
   - `git grep -in probe sidecar/internal/supervisor` — **empty**. Inside the package the word only
     ever means the registered body, so the rule decides every one of its ~150 comment uses, and
     this is what enforces it.
   - `git grep -in 'probe engine\|internal/probe\|probe\.Probe' -- . ':!docs/adr'` — empty.
   - `git grep -in '\bengine\b' sidecar/internal/supervisor sidecar/internal/clustersvc` — empty;
     the word has no other referent in those two trees.
   - `git grep -n 'mirrorEngine\|discoveryEngine' -- docs sidecar` — empty.
4. `gofmt -l`, `go vet ./...`, `go test -race ./...` — all green with no test changed beyond the
   names, since nothing behaves differently. Read the diff once for a line that is not a rename.

One commit. It touches 13 Go files by import, roughly 155 selector references (most of which —
`Result`, `Snapshot`, `Fail`, `Pass`, `Verdict*` — change only their package prefix), 9 `Run`
methods (five in `kubeconn/probe.go`, `sessionScoped` in `kubesync/discovery.go`, three in the
package's own tests), and the docs above.

## Not in this pass

- `Observation`/`Snapshot`. An observation names what a fetch commits, and the mirror's handle is
  not one; the word is still right for every body that exists, and the mirror spec can decide
  whether it needs another when it has a value to name.
- Any behavior. `Remove`/`Close` handing back values is the mirror spec's step 1, not this.

## Done when

`internal/probe` no longer exists, the four greps in step 3 come back empty, the suite is green,
the ADR is indexed, and the mirror spec reads in the new vocabulary. Then delete this file and its
README row.
