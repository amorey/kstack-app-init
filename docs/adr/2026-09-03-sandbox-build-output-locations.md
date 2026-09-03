---
title: Isolate Linux sandbox build outputs, with Cargo's target dir on the overlay
date: 2026-09-03
scope: repo
status: Accepted
---

# Isolate Linux sandbox build outputs, with Cargo's target dir on the overlay

Supersedes [Sandbox build-output isolation](2026-08-09-sandbox-build-separation.md), which placed
every relocatable output — Cargo's included — under `.sandbox-linux/`.

## Context

Development happens inside a Linux sandbox that bind-mounts this repo from a macOS host, so both
platforms share every build-output path. A `pnpm install` in the sandbox overwrites the host's
native macOS binaries in `node_modules`, and vice versa; Rust target dirs and Vite's `dist` collide
the same way. Some outputs are relocatable by configuration; `node_modules` and `dist` are fixed
paths tooling can't move.

Putting all of them under `.sandbox-linux/` keeps the two platforms apart and keeps artifacts on
the host's roomy disk, where they survive a sandbox recreate. But that directory is on the
bind-mounted host filesystem, and its directory metadata is not coherent under a dense writer:
Cargo gets `EEXIST` from `mkdir` for directories it has just created, and reads
`.fingerprint/*/invoked.timestamp` back missing right after writing it. Each retry gets one
directory further, so a build there makes progress without ever converging — 40 consecutive
attempts moved from `num-traits` to `gio-sys`. The effect is that `make test-rust` cannot run in
the sandbox at all, which leaves every Rust change made there unverified. No other output in the
tree is written densely enough to hit it.

## Decision

`scripts/sandbox-dev-setup.sh` — idempotent, a no-op outside the sandbox, run automatically by a
`SessionStart` hook in `.claude/settings.json` — separates the two platforms. It points the pnpm
store at `.sandbox-linux/` via the persistent env file and **bind-mounts**
`.sandbox-linux/{node_modules,dist}` over the fixed paths. A bind mount is sandbox-only: the host's
macOS `node_modules` sits untouched underneath.

**`CARGO_TARGET_DIR` is the exception: it points at `/home/agent/kstack-build/target`, on the
sandbox's own overlay**, which is coherent. `KSTACK_SANDBOX_CARGO_ROOT` overrides it, matching the
existing `KSTACK_SANDBOX_BUILD_ROOT` knob for everything else.

Go's caches need no handling (they default to `$HOME`, which isn't shared), nor does
`src-tauri/binaries/` (the sidecar filename carries the target triple, so both platforms' builds
coexist).

It must run before any build, test, or install command in the sandbox.

## Alternatives considered

**Leaving Cargo on the host mount and retrying the build.** Rejected: the loop makes progress but
does not converge, so it is not a workaround, only a way to recognise the fault.

**Moving every output to the overlay.** Rejected: it relocates `node_modules` and `dist`, which
work today, and spends the overlay's 20G budget on them. A debug `target/` alone is ~7G; adding
the rest would make the cap the routine failure instead of a distant one.

**Cloning the repo into the sandbox instead of bind-mounting.** Rejected: edits would need a sync
step to reach the host, defeating the shared-working-tree workflow.

**Relocating everything by configuration.** Not possible: npm/pnpm resolve `node_modules` by
directory walk and Vite's `dist` is path-conventional; no supported knob moves them per-OS.

## Consequences

`make test-rust` runs in the sandbox. The cost is that Rust artifacts now live on the overlay:
they are wiped when the sandbox is recreated (a cold rebuild is ~2 minutes) and they count against
its 20G cap, so a `target/` that has grown across profiles is worth deleting rather than keeping.

The obligation stays temporal: the script must have run before any install/build in the sandbox —
hence the SessionStart hook, and re-running it manually is always safe.
