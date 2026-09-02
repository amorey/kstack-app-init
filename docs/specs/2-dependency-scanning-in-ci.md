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
| `go-audit` | `Go · Audit` | `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` in `./sidecar` |
| `rust-audit` | `Rust · Audit` | `cargo install --locked cargo-audit` then `cargo audit` in `./src-tauri` |
| `typescript-audit` | `TypeScript · Audit` | `pnpm audit --audit-level high` at the repo root |

Notes for whoever builds it:

- `govulncheck` reports only vulnerabilities in code paths that are actually reachable, so it is
  low-noise enough to be a gate from day one.
- `cargo audit` is slow to install from source; use the `rustsec/audit-check` action if the install
  step costs more than a minute of CI.
- `pnpm audit` is the noisiest of the three. Start at `--audit-level high` so the gate is
  believable; lower it later if the noise turns out to be manageable.
- A finding with no fix available blocks the branch. That is the point — but give the team an
  escape hatch: `cargo audit` reads `.cargo/audit.toml` and `pnpm` reads `pnpm.overrides`; for Go,
  the only lever is upgrading. Any ignore entry gets a comment naming the advisory and the reason.

## Also: an SBOM at release

In the release workflow (not `ci.yml`), generate a CycloneDX SBOM for each ecosystem and attach it
to the GitHub release. Without it, a future advisory cannot be matched against a build that already
shipped. `anchore/sbom-action` covers all three trees in one step and is the cheapest thing that
works.

## Tests

The jobs are the test. Confirm each one fails on a known-bad input before merging — pin a
deliberately outdated dependency on a scratch branch, watch the job go red, then revert.

## When it lands

Add the row *"Dependency advisory scanning in CI"* to the protections table in
[`docs/security-model.md`](../security-model.md) as **Enforced**, naming the three jobs.
