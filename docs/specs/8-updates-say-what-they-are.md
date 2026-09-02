---
title: Updates say what they actually are
scope: host · docs · ci
status: Planned
---

# Updates say what they actually are

**Needs:** nothing. **Hands on:** nothing.

## Goal

Close the gap between the documented update story and the shipped one, and then close it in the
right direction.

`src-tauri/CLAUDE.md:7` says: *"In-app updates use the official `tauri-plugin-updater` (signed
bundles + hosted manifest)."* Neither `src-tauri/Cargo.toml` nor `src-tauri/tauri.conf.json`
declares the plugin or an update public key. A documented verification step that does not exist is
worse than a missing one nobody assumed.

What `release.yml` actually ships today:

- macOS: a `.dmg` signed with the Developer ID certificate and notarized, the sidecar signed
  inside it.
- Windows: an `.msi` signed through SignPath.
- Linux: `.deb`, `.rpm` and `.AppImage`, unsigned.
- Beside all of them, `SHA256SUMS` with a detached GPG signature from the release bot key.

So a download is verifiable by hand on every platform, and by the OS on two. What is missing is
the in-app path: nothing checks for a newer build, and nothing would verify one.

## Part one — do this now

Correct `src-tauri/CLAUDE.md`. Replace the in-app-updates sentence with what is true: bundles are
signed direct downloads (notarized on macOS, SignPath-signed on Windows, GPG-signed checksums for
all), and updating means downloading a new build by hand. Two sentences at most.

This part is five minutes and should not wait for the rest.

## Part two — land the updater

The release pipeline exists, so this can be built now.

1. Generate a keypair with `pnpm tauri signer generate`. The **private key never enters the repo**
   — it lives in the `production` environment's secrets (`TAURI_SIGNING_PRIVATE_KEY` and
   `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`), which the three bundle jobs already run under.
2. Add `tauri-plugin-updater` to `Cargo.toml` and register it in `lib.rs`. **The host drives the
   update** — check on launch, prompt, install — so `src-tauri/capabilities/default.json` gains
   nothing. Granting `updater:default` to the webview would let a compromised page trigger
   downloads and installs; a validly signed build is all it could install, but the surface is not
   the page's to have.
3. In `tauri.conf.json`, declare `plugins.updater` with the **public** key and an `https`
   endpoint for the manifest — the GitHub release's `latest.json` is the cheapest home. The public
   key in the bundle is what makes a substituted download fail to install; the endpoint's TLS is
   not what protects it.
4. Set `bundle.createUpdaterArtifacts: true`. The bundle jobs then emit a `.sig` beside each
   updater-capable artifact (`.dmg`/`.app.tar.gz`, `.msi`, `.AppImage` — `.deb` and `.rpm` update
   through the package manager, not in-app). The `release` job assembles `latest.json` from the
   signatures and publishes it with the bundles.
5. The sidecar ships inside the bundle as `externalBin`, so it is covered by the same signature.
   No separate update channel for it — the CLAUDE.md line about versioning them together stays
   true.

## Tests

Nothing unit-testable. The check is a manual one, done once when part two lands: install an older
build, publish a signed newer one to a test release, confirm it updates; then publish one signed
with a different key and confirm it is refused.

## When it lands

Part one: the CLAUDE.md sentence is the deliverable. Part two: move *"Signed in-app updates"* in
[`docs/security-model.md`](../security-model.md) out of **Not built** to **Held by review**, and
restore the CLAUDE.md sentence to its original claim — which will by then be accurate.
