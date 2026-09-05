---
title: Logout's revocation is best-effort, and the model says so
scope: sidecar, docs
status: Planned
---

# Logout's revocation is best-effort, and the model says so

**Needs:** nothing. **Hands on:** nothing. Closes R-10's remaining half.

## Goal

Stop the app implying that signing out killed the server-side session.

`Logout` does two things, and only the first is guaranteed. It clears the local credential, then
hands the refresh token to a goroutine that tries to revoke it
(`internal/auth/auth.go::Logout`, `revokeAsync`). The order is deliberate — a user signing out
must not wait on an unreachable server — but it means a failed revoke, or a quit before the
goroutine finishes, leaves the grant live on the server while the app says signed out.

Nobody is told this, and the ordering that makes it safe is not pinned by a test: nothing would
catch a change that revoked before clearing, or that reported signed out on a failed clear.

## What is true now

- Local clear happens first and its failure keeps the session signed **in**
  (`TestLogoutKeepsSessionWhenCredentialClearFails`). A failed clear also returns before
  `revokeAsync`, so nothing is revoked either: the grant stays live on both sides, which is the
  consistent outcome but is nowhere written down.
- Revocation is fired and forgotten under a 10-second budget (`revokeTimeout`), on a context that
  a returning mutation cannot cancel.
- The service holds no long-lived goroutines and has no `Close`, so **shutdown does not wait for a
  revoke in flight**. The sidecar exits on stdin EOF; quitting the app right after signing out can
  end the process mid-request.
- Two tests already cover the happy path: `TestLogoutRevokesToken` and
  `TestLogoutDoesNotBlockOnRevoke`.

## Design

**Nothing about the behaviour changes.** This is the right trade and the ADR-free kind of
best-effort: the alternative is a sign-out that hangs. What is missing is a written statement and
two tests that hold the halves apart.

**Say it in the model.** `security-model.md` already carries the *Sign-out clears the local
credential* row, which states the best-effort half in prose. This spec's job is to move it from
**Built** to **Enforced**, naming the two tests below. The *Sidecar → cloud* boundary paragraph,
which already says cloud sign-out does not revoke Kubernetes credentials, gains the same sentence
about its own grant.

**Test the halves apart.** One test in `internal/auth/auth_test.go`, driven by a fake OAuth client
whose `Revoke` is channel-controlled — no wall clock. It carries both assertions, because they are
one property:

- `Revoke` records whether `clear` has already run, and asserts it has. That is the ordering the
  whole design rests on, and it is what nothing pins today.
- `Revoke` then returns an error, and the session is still signed out with `Logout` returning nil.

`TestLogoutDoesNotBlockOnRevoke` already covers the timing; this covers the order and the error
path it drops.

## Not in this pass

- **Waiting for the revoke at shutdown.** Making the sidecar's exit block on an in-flight revoke
  turns a network hang into a slow quit — the same trade this design rejected on the mutation path,
  moved to a worse place.
- **Retrying a failed revoke.** A retry queue needs the token to survive the clear that just erased
  it, which is a durable copy of the credential we are trying to get rid of. If it is ever wanted,
  it is an ADR first.
- **Telling the user in the UI.** Worth doing, but it is a product decision about sign-out
  messaging, not a security control.

## Build order

One commit. Test first, then the two doc edits.

## When it lands

- `security-model.md`'s *Sign-out clears the local credential* row moves from **Built** to
  **Enforced**, naming the test, and stays explicit about the half that is not a promise.
- The **R-10** bullet leaves `TODO.md`'s security section entirely — nothing is left owed.
- Delete this spec.

## Done when

Sign out with the cloud endpoint unreachable: the app reports signed out immediately, the local
keyring entry is gone, and the log carries no stall. `go test ./internal/auth/...` covers both
halves, and `security-model.md` says which one is a promise.
