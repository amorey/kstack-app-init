---
title: A cache stops outliving the user's interest in its cluster
scope: sidecar
status: Planned
---

# A cache stops outliving the user's interest in its cluster

**Needs:** nothing. **Hands on:** nothing.

## Goal

A cluster's mirrored contents should not survive indefinitely after the user stops looking at that
cluster. This is the one obligation left open by
[the cache is ordinary application data](../adr/2026-09-02-the-cache-is-ordinary-application-data.md):
everything else about the cache is protected as well as the kubeconfig beside it, but a token
expires and a certificate is revoked while the file keeps answering.

## What to build

1. **Evict a cache whose cluster has not been opened in N days.** The manager already owns the
   file's whole lifetime — `Manager.Remove` closes the file and unlinks it and its `-wal`/`-shm`
   sidecars, and refuses a later open of the same id — so eviction is a policy above it, not new
   teardown machinery. What it needs is a last-opened timestamp per cache, written where the cache
   record already lives rather than inside the cache file it is about to delete.
2. **Clear every cache on sign-out.** Sign-out is the user saying the machine no longer speaks for
   them; a mirror that answers afterwards contradicts that. `internal/auth`'s `Logout` is the hook.
3. **Choose N, and say why in the spec before building.** It trades a cold relist on return
   against how long a revoked credential's answers persist.

## Constraints

- **Eviction is not a failure.** A cluster the user opens again after eviction rebuilds from the
  cluster like any first sync; nothing may treat a missing file as an error state in the UI.
- **A claim still out must not resurrect the file.** `Remove`'s existing retirement discipline is
  the reference — the decision is recorded first, the unlink retried after.

## When it lands

Move the *"A retention policy…"* row in [`security-model.md`](../security-model.md) from **Not
built** to **Enforced**, naming the test, and fold the policy itself into `sidecar/CLAUDE.md`.
