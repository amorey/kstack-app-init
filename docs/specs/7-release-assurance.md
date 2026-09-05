---
title: Release assurance
scope: ci
status: Planned
---

# Release assurance

**Needs:** nothing. **Hands on:** a release whose provenance can be checked — specs
[7](8-macos-entitlements.md) and [8](9-signed-in-app-updates.md) both rest on it. Closes R-08 and
the rest of X-1.

## Goal

Make a release something a user can verify, and make the pipeline that produced it hard to divert.

Four gaps, all in `.github/`:

**Actions float.** Every third-party action is pinned to a tag or a branch, and a tag is mutable.
The worst of them is `dtolnay/rust-toolchain@master` — a branch, in the job that builds the binary
we ship. One action is already pinned properly
(`anchore/sbom-action@3ad7283…`), which is the pattern the rest should follow.

**Release permissions are broad.** `release.yml` grants `contents: write` and `deployments: write`
at the top level, so every job in it — including ones that only build — runs with the token that
can publish a release.

**Signing is configured but unverified.** The workflow wires macOS notarization, SignPath for the
MSI and a GPG-signed `SHA256SUMS`. Whether the artifacts that come out are actually signed, and
what a user should run to check them, is not established anywhere. Linux bundles are unsigned.

**Scans only run on pull requests.** `ci.yml`'s trigger is `pull_request` plus
`workflow_dispatch`. The three audit jobs are good — npm fails at moderate, `cargo audit` denies
unsound and unmaintained against a reasoned ignore list,
[the advisories Tauri pins](../adr/2026-09-05-tauri-pinned-transitive-advisories.md) are listed with
reasons — but an advisory published against a quiet `main` is discovered by nobody until the next
pull request. Releases do not depend on these jobs at all.

## Design

### Pin every action to a SHA

Each third-party `uses:` becomes `owner/repo@<40-char sha> # vX.Y.Z`, including the ones in
`.github/actions/setup-environment`. The comment is what makes the pin readable; the SHA is what
makes it a pin. **Local actions (`uses: ./.github/actions/…`) are not pinned** — they are this
repository, at this commit, already.

`dtolnay/rust-toolchain@master` is the one to handle first and deliberately: it is a branch, in the
build path. It also breaks the comment convention — that action publishes no version tags, only
branch refs, so its pin carries a dated comment (`# master @ 2026-09-05`) instead of a version, and
Dependabot will treat it differently from the rest. Expect to review it by hand.

Pinning without an update path rots, so this adds `.github/dependabot.yml` — the repo has none —
with the `github-actions` ecosystem. Dependabot then raises a pull request that moves the SHA and
its comment together.

### Least privilege per job

`release.yml` drops to `permissions: {}` at the top, and each job declares what it needs. Only the
final `release` job gets `contents: write`. **`deployments: write` is used by nothing** — no job
creates a deployment — so it is deleted rather than reassigned. `ci.yml` already reads
`contents: read` top-level; leave it.

### A scheduled scan

The three audit jobs gain a `schedule` trigger (daily) so an idle branch still learns about a new
advisory. The scanning half of X-1 is otherwise already done and stays as it is.

### Releases depend on the checks

The `release` job gains a gate that the required checks passed **for the exact commit being
released**. The mechanism is a step that queries the check runs for that commit and fails if any
required one is missing or failed — a `needs:` on another workflow does not express "this commit".

**The commit is the tag's, not `github.sha`.** Every job checks out
`refs/tags/desktop/v${{ needs.config.outputs.version }}`, and on a `workflow_dispatch` run
`github.sha` is the commit of whatever ref was dispatched from, which need not be that tag. The
gate resolves the tag to a commit and queries *that*, or it is checking a different build than the
one being shipped.

### Say how to verify

`docs/` gains a short verification page and the release body links it: the GPG key and where it
comes from, `gpg --verify SHA256SUMS.asc SHA256SUMS`, `shasum -c SHA256SUMS`, and per platform —
`codesign --verify --deep --strict` plus `spctl -a -t exec` on macOS, `signtool verify` on Windows,
and for Linux, plainly, that the bundle is unsigned and the checksum is the check.

An SBOM is already generated per release (`anchore/sbom-action`, CycloneDX). The page says it
exists and what it covers.

## Rules

- **Every `uses:` is a SHA.** A tag is a moving target.
- **A job gets the smallest token that lets it finish.**
- **A release is gated on checks for its own commit.**
- **What we tell users to verify, we have run ourselves once**, on real artifacts.

## Build order

Each is one commit and independently useful.

1. Pin every action; add `.github/dependabot.yml` with the `github-actions` ecosystem.
2. Per-job permissions in `release.yml`.
3. Scheduled audit runs.
4. The commit-exact check gate on the release job.
5. The verification page, written against a real dry-run release and its actual output.

## Not in this pass

- **Branch protection and required-check settings.** They live in GitHub's settings, not in this
  checkout, and no file here can establish them. They stay listed in the review's verification debt,
  and the check gate above is what this repo can enforce on its own.
- **Reproducible builds.** A much larger project, and not what R-08 asked for.
- **Signing Linux bundles.** Real work with no obvious trust root for this project yet; it belongs
  with [signed in-app updates](9-signed-in-app-updates.md), which needs a signing story anyway.
- **Trimming the macOS entitlements.** [Spec 7](8-macos-entitlements.md), which needs the signed
  build this one makes verifiable.

## When it lands

- `security-model.md`'s advisory row loses "Advisories run per pull request, not on a schedule, and
  releases do not depend on these jobs (R-08)". New rows: *Third-party actions pinned to SHAs* and
  *A release is gated on the checks for its own commit*.
- The **R-08** bullet leaves `TODO.md`; the verification-debt paragraph keeps only what needs real
  hardware.
- Delete this spec.

## Done when

A dry-run release produces artifacts, and someone who is not the person who built them follows the
verification page start to finish on each platform and gets a clean result. A grep for `uses:`
finds no tag. `main` has run a scheduled audit with nobody opening a pull request.
