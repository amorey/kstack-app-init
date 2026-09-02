---
title: CI scans all three dependency trees for advisories
scope: ci
status: Planned
---

# CI scans all three dependency trees for advisories

**Needs:** nothing. **Hands on:** nothing.

## Goal

Fail a pull request that pulls in a dependency with a known vulnerability.

The app ships three dependency trees — Go, Rust, npm — plus a bundled Go binary inside a Rust
bundle. `ci.yml` lints, vets, tests and builds all three and scans none of them. A compromised
dependency is the most likely route to the two worst outcomes in the threat model: anything running
in the webview reaches the whole cluster surface, and anything running in the sidecar reaches the
user's kubeconfig.

## What to build

Three new jobs in `.github/workflows/ci.yml`, each shaped like the existing `go-vet` /
`typescript-lint` jobs: `runs-on: ubuntu-24.04`, `actions/checkout@v6`, then
`./.github/actions/setup-environment` with the right toolchain input.

| Job | Name | Command |
| --- | --- | --- |
| `go-audit` | `Go · Audit` | `go run golang.org/x/vuln/cmd/govulncheck@<pin> ./...` in `./sidecar` |
| `rust-audit` | `Rust · Audit` | `cargo install --locked cargo-audit --version <pin>` then `cargo audit` in `./src-tauri` |
| `typescript-audit` | `TypeScript · Audit` | `pnpm audit --audit-level high` at the repo root |

Notes for whoever builds it:

- **Pin every scanner to an exact version**, and bump deliberately. A scanner is a program from
  the internet running in CI with the repository checked out; `@latest` hands that to whoever
  publishes next. `<pin>` above is the release current when this lands — look it up, do not guess.
  The Go module proxy verifies `govulncheck` against the checksum database, and `cargo install
  --locked` pins its own lockfile, so the version is the only thing left to fix.
- Third-party actions are pinned by **commit SHA**, not tag — a tag can be moved. The `actions/*`
  set is GitHub's own and keeps the `@vN` form the rest of the file uses.
- `govulncheck` reports only vulnerabilities in code paths that are actually reachable, so it is
  low-noise enough to be a gate from day one.
- `cargo audit` is slow to install from source; use the `rustsec/audit-check` action (SHA-pinned)
  if the install step costs more than a minute of CI.
- `pnpm audit` is the noisiest of the three. Start at `--audit-level high` so the gate is
  believable; lower it later if the noise turns out to be manageable. Scan dev dependencies too:
  codegen, the bundler and the test runner all execute in CI and on every developer machine.
- A finding with no fix available blocks the branch. That is the point — but give the team an
  escape hatch that is reviewed rather than silent: `cargo audit` reads `.cargo/audit.toml`
  (`[advisories] ignore`), `pnpm` reads `pnpm.auditConfig.ignoreCves` / `ignoreGhsas` in
  `package.json`; for Go, the only lever is upgrading. Every ignore entry carries a comment naming
  the advisory and the reason, and this file says so in its `## Tests` section so a reviewer knows
  to look. `pnpm.overrides` is a fix (it forces a version), not an ignore.

## Also: an SBOM at release

`release.yml` already exists. In its `release` job, generate a CycloneDX SBOM for each ecosystem
and attach it beside the bundles. Without it, a future advisory cannot be matched against a build
that already shipped. `anchore/sbom-action` (SHA-pinned) covers all three trees in one step from
the lockfiles — and the `release` job does not check the repository out today, so add an
`actions/checkout@v6` at the release tag ahead of it.

## Tests

The jobs are the test. Confirm each one fails on a known-bad input before merging — pin a
deliberately outdated dependency on a scratch branch, watch the job go red, then revert. The
ignore files are part of the review surface from then on: an entry without a named advisory and a
reason is a change to reject.

## When it lands

Move the row *"Dependency advisory scanning in CI"* in [`docs/security-model.md`](../security-model.md)
from **Not built** to **Enforced**, naming the three jobs.
