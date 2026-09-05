---
title: The macOS bundle asks for the entitlements it needs
scope: src-tauri, ci
status: Planned
---

# The macOS bundle asks for the entitlements it needs

**Needs:** [release assurance](7-release-assurance.md) — this cannot be finished without a signed
build whose provenance is checkable. **Hands on:** nothing. Closes H-3.

## Goal

Stop shipping three hardened-runtime exceptions we have never shown we need.

`src-tauri/entitlements.plist` grants four things. One is uncontroversial:

- `com.apple.security.network.client` — the OAuth loopback and the sidecar's cloud connection.

The other three each weaken the hardened runtime, and the file's own comments already admit their
necessity is unverified:

- `com.apple.security.cs.allow-jit`
- `com.apple.security.cs.allow-unsigned-executable-memory`
- `com.apple.security.cs.disable-library-validation`

`allow-unsigned-executable-memory` is the broadest: it lets the process map writable-executable
memory, which is the thing the hardened runtime exists to prevent. `disable-library-validation`
lets the process load libraries signed by any team.

## What is true now

- The bundle sets `minimumSystemVersion: 15.0` and points at `entitlements.plist`
  (`src-tauri/tauri.conf.json`).
- The sidecar is a **separate executable**, spawned as a Tauri sidecar. Library validation governs
  code loaded *into* the process; it is not what permits spawning a separately signed child. That
  distinction is why the review would not accept "we spawn a sidecar" as the justification.
- WKWebView runs JavaScript in its **own** process. Whether the app process needs JIT entitlements
  for it is exactly the question — and the answer differs depending on whether `macOSPrivateApi` is
  on, which for this app it is.
- Nobody has run the app without these entitlements.

## Design

**This spec is an experiment, not a change.** The deliverable is knowing which of the three are
load-bearing, and a file that grants only those.

The work is a matrix, run on a **signed, notarized** build — an ad-hoc-signed local build does not
enforce the hardened runtime the same way, so a local pass proves nothing:

| Build | Entitlements |
| --- | --- |
| A | all four (today) |
| B | network only |
| C | network + `allow-jit` |
| D | network + `allow-jit` + `allow-unsigned-executable-memory` |

Start at B. If B works, the other three grants go. If B crashes, the crash report names the reason,
and each step up is one grant with one reason recorded beside it.

**What "works" has to mean, precisely** — the paths that plausibly touch JIT or library loading:

- Launch, and every window opens and paints. **On both CPU architectures** — the binary is
  universal and signed once, but JIT behaviour is not architecture-independent. The macOS job runs
  on `macos-26` (Apple silicon), so who runs the x86_64 half is an open question this experiment
  has to answer, not inherit.
- A cluster syncs, a table scrolls, the dashboard renders.
- Sign-in through the system browser completes, and the tray reflects it.
- The sidecar spawns, is reached, and shuts down cleanly.
- A second window, and a reload.

**Whatever survives keeps its comment, rewritten.** Each remaining entitlement gets one line saying
what broke without it and on which macOS version — a reason a future reader can re-check, replacing
today's "necessity is unverified".

## Rules

- **A grant with no recorded failure behind it is removed.**
- **The test is a signed, notarized build.** Local ad-hoc signing does not answer this question.
- **Record the macOS version.** An entitlement needed on one release may not be on the next; without
  the version the note cannot be re-tested.

## Build order

0. **Give `release.yml` a way to build something that is not a tag.** Every job checks out
   `refs/tags/desktop/v<version>`, so as it stands each variant below means pushing a real release
   tag. Add an optional `ref` input that the checkout steps prefer when set, or accept that the
   experiment runs on throwaway tags and say so. Either way it is decided before step 1, because
   otherwise step 1 stalls on its first day.

1. Produce a signed dry-run build at variant B (`release.yml` supports a dry run with the
   test-signing policy).
2. Walk the checklist. Record the outcome — including a clean pass — in a dated note under
   `docs/security/`, since that directory is the append-only record of what was actually observed.
3. Step up one grant at a time only as failures require.
4. Land the trimmed `entitlements.plist` with a reason per surviving key.

## Not in this pass

- **Sandboxing the app** (`com.apple.security.app-sandbox`). A much larger change: the sidecar,
  its data directory and the kubeconfig read all become sandbox questions.
- **Windows and Linux hardening flags.** Different mechanisms; no finding open against them.
- **Removing `macOSPrivateApi`.** It is what makes the transparent window frame work, and it is a
  product decision, not a security finding.

## When it lands

- `security-model.md` gains a row: *The macOS bundle grants only the hardened-runtime exceptions a
  signed build was shown to need* — **Held by review**, since nothing automated re-checks it, and
  linking the dated note.
- The **H-3** bullet leaves `TODO.md`; the review's H-3 row is answered by the note.
- Delete this spec.

## Done when

A signed, notarized build passes the whole checklist with an `entitlements.plist` that grants only
what a recorded failure justified, and each remaining key names its reason and the macOS version it
was observed on.
