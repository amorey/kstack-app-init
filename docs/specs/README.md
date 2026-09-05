# Specs

A spec describes **what we are about to build**, in enough detail to implement it and no more.
It is a plan with a shelf life.

The other two doc kinds keep their jobs: `CLAUDE.md` says what is true now, `docs/adr/` says why a
design was chosen. A spec says what comes next.

## Filenames

```
docs/specs/<n>-short-slug.md   a spec with a place in a sequence
docs/specs/short-slug.md       one that stands alone
```

No dates — a spec is edited as the work moves, not appended to. **The number is the build order**,
so the directory listing is the plan; a spec with no place in a sequence goes without one.
Renumber when the plan changes, and fix the links in the same edit — the numbers are load-bearing
only as far as they agree with each spec's own header.

## Lifecycle

Update the spec while the work is in progress. When it lands, fold what is now true into the
relevant `CLAUDE.md`, write an ADR if a decision needs its reasons recorded, and delete the spec.
A spec left behind after the code ships is a second source of truth.
A security spec also has a line in [`TODO.md`](../TODO.md#security) and a row in
[`security-model.md`](../security-model.md); its own *When it lands* section says where the row moves.

## Index

Each spec is self-contained: read one and you can build it. Each header states what it needs and
what it hands on.

The numbered specs are the open security findings from the
[4 September review](../security/2026-09-04-security-review.md), ordered cheapest first. Only the
release chain has real dependencies: 7 makes a release verifiable, which 8 and 9 both rest on. 9
also needs the manifest 3 builds, and is the one spec whose first deliverable is an ADR rather than
code.

| Spec | Scope | Status |
| --- | --- | --- |
| [1 — Logout's revocation is best-effort, and the model says so](1-logout-revocation-is-best-effort.md) | sidecar, docs | Planned |
| [2 — The native webview refuses to navigate](2-native-navigation-policy.md) | src-tauri | Planned |
| [3 — The app says when it is out of date](3-update-notification.md) | sidecar, src-tauri | Planned |
| [4 — Diagnostics are redacted and bounded](4-diagnostics-and-malformed-credential-fields.md) | sidecar | Planned |
| [5 — The host authenticates the sidecar](5-host-authenticates-the-sidecar.md) | src-tauri, sidecar | Planned |
| [6 — Resource budgets](6-resource-budgets.md) | sidecar, src-tauri | Planned |
| [7 — Release assurance](7-release-assurance.md) | ci | Planned |
| [8 — The macOS bundle asks for the entitlements it needs](8-macos-entitlements.md) | src-tauri, ci | Planned |
| [9 — Signed in-app updates](9-signed-in-app-updates.md) | src-tauri, sidecar, ci | Planned — design first |
| [Connection throughput](connection-throughput.md) | sidecar | Planned |
| [A burst of object frames reads its bodies in one statement](batched-body-reads.md) | sidecar | Planned |
