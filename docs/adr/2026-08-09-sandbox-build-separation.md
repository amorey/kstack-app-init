---
title: Isolate Linux sandbox build outputs under .sandbox-linux via a setup script
date: 2026-08-09
scope: repo
status: Accepted
---

# Isolate Linux sandbox build outputs under .sandbox-linux via a setup script

## Context

Development happens inside a Linux sandbox that bind-mounts this repo from a macOS host, so
both platforms share every build-output path. A `pnpm install` in the sandbox overwrites the
host's native macOS binaries in `node_modules`, and vice versa; Rust target dirs and Vite's
`dist` collide the same way. Some outputs are relocatable by configuration; `node_modules`
and `dist` are fixed paths tooling can't move.

## Decision

`scripts/sandbox-dev-setup.sh` — idempotent, a no-op outside the sandbox, run automatically
by a `SessionStart` hook in `.claude/settings.json` — separates the two platforms. It points
the relocatable outputs (`CARGO_TARGET_DIR`, the pnpm store) at `.sandbox-linux/` via the
persistent env file, and **bind-mounts** `.sandbox-linux/{node_modules,dist}` over the fixed
paths. A bind mount is sandbox-only: the host's macOS `node_modules` sits untouched
underneath. Go's caches need no handling (they default to `$HOME`, which isn't shared), nor
does `src-tauri/binaries/` (the sidecar filename carries the target triple, so both
platforms' builds coexist).

It must run before any build, test, or install command in the sandbox.

## Alternatives considered

**Cloning the repo into the sandbox instead of bind-mounting.** Rejected: edits would need a
sync step to reach the host, defeating the shared-working-tree workflow.

**Relocating everything by configuration.** Not possible: npm/pnpm resolve `node_modules` by
directory walk and Vite's `dist` is path-conventional; no supported knob moves them per-OS.

**Per-platform install prefixes / postinstall switching.** Rejected: fragile against every
tool that hardcodes the conventional paths, and easy to half-apply.

## Consequences

Both platforms build in the same tree without clobbering each other. The obligation is
temporal: the script must have run before any install/build in the sandbox — hence the
SessionStart hook, and re-running it manually is always safe.
