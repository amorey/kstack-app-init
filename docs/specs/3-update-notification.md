---
title: The app says when it is out of date
scope: sidecar, src-tauri
status: Planned
---

# The app says when it is out of date

**Needs:** nothing — deliberately. **Hands on:** the version endpoint that
[signed in-app updates](9-signed-in-app-updates.md) also reads. Closes the half of H-4 that is the
actual risk.

## Goal

Tell a user their app is old. Nothing does today.

There is no updater and no notification: `tauri.conf.json` declares no updater plugin and
`Cargo.toml` has none. Someone who installed once stays on that version until they happen to visit
the releases page — including through a security fix. H-4 reads as "no updater", but the harm is
narrower and simpler than that: **a user who does not know.**

Telling them needs no signing key, no trust root and no install path, so it does not wait for the
release chain. It is the highest value in this set per unit of work, and it is the only part of H-4
that helps a Linux user, whose bundles are unsigned and always will be from our side.

## Design

The sidecar fetches a small manifest on a slow cadence, compares versions, and publishes the result
on the existing GraphQL surface. The webview renders a quiet notice with a link that opens the
release page **in the system browser**, through the host's opener — the path the tray already uses
(`src/tray/mod.rs`). Nothing is downloaded and nothing is installed.

**The check lives in the sidecar** because it already owns every outbound HTTPS call and the
backoff around them, and the webview has no network access at all.

### What it fetches

The release's static `latest.json` asset, which
[spec 9](9-signed-in-app-updates.md) needs regardless — so this builds the endpoint that one
consumes, rather than inventing a second. It is a release asset, not a GitHub API call: the
unauthenticated API is rate-limited per IP, and a lab or office behind one NAT would exhaust it.

The request sends the current version and nothing else. No identity, no machine id, no cluster
information.

### Cadence

On launch, then every 24 hours, **jittered** — every copy of the app checking at the same wall-clock
moment is a self-inflicted thundering herd. A failed check is silent: it retries on the next
interval and never becomes an error banner. Not knowing about an update is the state the user is
already in.

### The default is on, and it is written down

This is a network call the user did not ask for, so it is a real decision and this spec makes it:
**on by default, stated plainly** in the settings dialog and in `security-model.md`, with a toggle
to turn it off that persists in `host.json` like every other preference.

Off-by-default fails the goal. The user who never finds the toggle is exactly the user still
running the version with the bug — an update notice nobody enabled is not a notice.

## Rules

- **No identity is sent.** Version alone.
- **A failed check is silent.** It is not an error the user can act on.
- **The check is off if the user says so**, and it says so before they have to ask.

## Build order

1. Version comparison as a pure function, table-tested: older, equal, newer, a prerelease, and a
   malformed string. This is where an off-by-one becomes a nag loop, so it is worth its own commit.
2. The fetch, its cadence and its jitter, paced by parameter rather than by constant so the test
   never waits on the real interval (`prefsync`'s `withBackoff` is the shape).
3. The GraphQL field and the webview notice.
4. The settings toggle, and the sentence in the settings dialog that says what it sends.

## Not in this pass

- **Downloading or installing anything.** [Spec 9](9-signed-in-app-updates.md), which needs a
  signing key and a trust root this one does not.
- **Release notes in the app.** A link to the release page is the whole surface.
- **Telling the user *which* fixes they are missing.** That is a changelog feed, and it is a
  product feature, not a security control.

## When it lands

- `security-model.md`'s *Signed in-app updates* row splits: notification becomes its own row,
  **Built**, naming what it sends and that it is on by default; the apply half stays **Not built**
  against spec 9.
- The **H-4** bullet in `TODO.md` narrows to the apply half.
- Delete this spec.

## Done when

An app built one version behind a published release says so within one check interval, on all three
platforms, and the link opens the release page in the system browser. Turning the toggle off stops
the request — confirmed by there being no outbound connection, not by the notice being hidden.
