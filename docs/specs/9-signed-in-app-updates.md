---
title: Signed in-app updates
scope: src-tauri, sidecar, ci
status: Planned — design first
---

# Signed in-app updates

**Needs:** [release assurance](7-release-assurance.md), for a release whose artifacts can be
verified; [macOS entitlements](8-macos-entitlements.md), so the bundle being updated is the one we
intend to ship; and [update notification](3-update-notification.md), which builds the manifest this
reads. **Hands on:** nothing. Closes the apply half of H-4.

**This spec is not ready to build.** The trust root, key rotation, Linux and rollback are open
questions, and its first deliverable is an ADR that settles them — see *Build order*. Everything
below is the shape that ADR argues about.

## Goal

Give a user a safe way to install the update [spec 3](3-update-notification.md) told them about.

Knowing is the half that closes the risk, and it ships first and separately. This is the
convenience half — and the half that, done wrong, hands an attacker the install path. The bar is
therefore higher than "it works": a signed artifact from a key we control, and a failure that
leaves the app running.

## What is true now

- `release.yml` builds Linux, macOS and Windows bundles, notarizes the macOS DMG, submits the MSI
  to SignPath, and publishes a GPG-signed `SHA256SUMS` alongside a CycloneDX SBOM.
- **Linux bundles are unsigned**, and the platform has no signing convention we already satisfy.
- `bundle.targets` is `"all"`, so the same tag produces every installer format — which matters
  below, because an updater must know which format it may replace.
- The app is a desktop bundle with a sidecar binary inside it; an update replaces both together.

## Design

`tauri-plugin-updater` is the mechanism, and most of the wiring is already decided:

- **The keypair** comes from `pnpm tauri signer generate`. The private half lives only in the
  `production` environment's secrets (`TAURI_SIGNING_PRIVATE_KEY` and its password) — the macOS and
  Windows bundle jobs already use that environment; Linux would have to be added. The **public** key
  goes in `plugins.updater` in `tauri.conf.json`, and it is what makes a substituted download fail
  to install. The endpoint's TLS is not.
- **Nothing is added to `src-tauri/capabilities/default.json`.** The host checks, prompts and
  installs. Granting `updater:default` to the webview would hand a compromised page the
  download-and-install path for no reason.
- **`bundle.createUpdaterArtifacts: true`**, after which the bundle jobs emit a `.sig` beside each
  updater-capable artifact (`.dmg`/`.app.tar.gz`, `.msi`, `.AppImage` — `.deb` and `.rpm` update
  through the package manager) and the `release` job assembles the manifest.
- **The manifest** is an `https` endpoint; the cheapest is the GitHub release's `latest.json`.
- **The sidecar rides the same signature** as an `externalBin`, so it needs no channel of its own.

What is left to design is the trust root around that key.

- **The signing key.** Losing control of it is worse than any finding this spec closes, so its
  storage, rotation and revocation are part of the design rather than an afterthought. Rotation is
  the hard half: the public key is compiled into installed apps, so a rotated key cannot update
  them.
- **Per platform:** macOS and Windows updates ride the already-signed artifacts, and the updater's
  signature is a second, independent check. **Linux is a choice, not a constraint** — the updater's
  own signature is independent of distro signing, so an unsigned `.deb` does not prevent an in-app
  update. The question is whether we should update a package the system's package manager believes
  it owns. The likely answer is that Linux keeps the notification and no in-app apply; either way
  it is a decision the ADR records, not a limitation to discover later.
- **Rollback and failure.** An interrupted update must leave a working app. What the plugin
  guarantees here has to be read and recorded, not assumed.

## Rules

- **An update is verified before it is applied**, by a key shipped in the app, not by transport
  security alone.
- **A failed update leaves the previous version working.**
- **The trust root is decided in an ADR before any of it is wired.**

## Build order

1. **An ADR settling the trust root**, before any code: where the private key lives, who can use
   it, what rotation means when the public key is compiled into installed apps, whether Linux
   applies updates, and what the plugin actually guarantees on an interrupted install (read, not
   assumed). This is the deliverable that makes the rest routine.
2. The signing key: generate, store, document rotation. A process commit as much as a code one.
3. The updater plugin, wired for macOS and Windows, with the manifest served from the release.
4. Linux, as the ADR decided.

**None of this is unit-testable.** The check is manual and done once: install an older build,
publish a signed newer one to a test release and confirm it updates, then publish one signed with a
different key and confirm it is refused.

## Not in this pass

- **The notification itself.** [Spec 3](3-update-notification.md).
- **Automatic background installation.** A desktop app that replaces itself without being asked is
  a different product decision, and a much larger blast radius when it goes wrong.
- **Delta updates.** An optimisation, and this is not fast enough to need it.
- **Signing Linux bundles.** Real work with no clear trust root here; if it happens it comes with
  its own ADR.
- **A staged rollout.** Needs infrastructure the project does not have.

## When it lands

- `security-model.md`'s *Signed in-app updates* row moves off **Not built** — to **Held by review**
  for the platforms that apply updates, since the check is manual, naming what remains true for
  Linux.
- `src-tauri/CLAUDE.md`'s distribution paragraph is rewritten; it says today that there are no
  in-app updates.
- The **H-4** bullet leaves `TODO.md`.
- Delete this spec.

## Done when

On macOS and Windows, applying an update produces a working signed app at the new version, and an
artifact signed with the wrong key is refused. Killing the app mid-update leaves the old version
working.
