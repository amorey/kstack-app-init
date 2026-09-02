---
title: Decide what the cluster cache is
scope: sidecar · decision
status: Planned
---

# Decide what the cluster cache is

**Needs:** nothing. **Hands on:** nothing.

## Goal

Answer one question, in writing: **is the cache ordinary application data, or is it
credential-bearing storage?**

This is a decision, not a patch. Everything else in the security TODO has an obvious shape; this one
does not, and the work depends entirely on the answer.

## The facts to decide against

- The cache holds full object bodies for every mirrored kind, one SQLite file per cluster.
- Write-time redaction is real and correct where it applies — `kubestore/objects.go` blanks Secret
  values, keyed off the body's own group and kind so it cannot be dodged by how the object was
  addressed — and the code says plainly that its table is **not exhaustive**. ConfigMaps, inline
  container env values, annotations on non-Secret kinds, and CRDs outside the listed entries are
  stored in the clear.
- The result answers, offline and with no RBAC check, questions that previously needed a live
  authenticated call. An attacker who copies the file gets that answer at their leisure.
- **The cache outlives the credentials.** A token expires, a certificate is revoked, an employee is
  offboarded — the file still answers. That is the one way it is *not* "the same as the
  kubeconfig beside it".
- The file is `0600` in a `0700` directory. That
  is the same protection the user's own kubeconfig has, and it protects against another account on
  the same machine and nothing else. A lost or stolen disk is protected by full-disk encryption or
  not at all; FileVault and BitLocker are on by default on current Apple and Windows hardware,
  Linux varies.
- Not everything can be hidden. The columns the store indexes and sorts on — names, namespaces,
  labels, status summaries, event reasons and messages — stay readable, and the events FTS index
  holds message text in the clear by construction. Encrypting the bodies changes what an attacker
  with the file learns; it does not make the file opaque.

## The two answers

**A — ordinary application data.** The cache is no more sensitive than the kubeconfig sitting
beside it, and file permissions plus the OS's disk encryption are the right level of protection
for a single-user desktop app. Write an ADR saying so. It must say what an attacker with the file
gets, why that is acceptable given that the same attacker can usually read the kubeconfig and just
ask the cluster, and what to do about the case where they cannot — the credentials-outlived
point above — which is a retention policy, and belongs in the answer either way.

**B — credential-bearing storage.** Then it needs, in this order:

1. **Encryption at rest**, with the key in the OS keyring — the same keyring the refresh token
   already uses (`internal/auth/keyring.go`). SQLCipher is not available through the pure-Go
   `modernc.org/sqlite` driver, so this means encrypting the stored bodies rather than the file:
   `rawcodec.go` already compresses every body on the way in and decompresses on the way out, so
   it is the one chokepoint both directions pass through, and a WAL page then carries ciphertext
   too. Write down what stays readable — the list in the facts above — and decide whether event
   search keeps its plaintext index or goes.
2. **A retention policy**, so a cluster's contents stop outliving the user's interest in them:
   evict a cache whose cluster has not been opened in N days, and clear every cache on sign-out.
3. **A key-loss path.** If the keyring entry goes, the cache is unreadable; it must rebuild from
   the cluster rather than wedge.

## How to decide

Write the ADR either way — that is the deliverable. `docs/adr/README.md` has the format. Then:

- **Answer A:** the ADR is the whole change. Update the *"Cache encryption at rest"* row in
  [`docs/security-model.md`](../security-model.md) to point at it instead of at this spec, so a
  future reviewer sees a decision rather than a gap. If the ADR calls for retention, that becomes
  its own spec.
- **Answer B:** the ADR states the decision and this spec is replaced by a new one for the build.

Do not start building encryption before the ADR exists. The cost is high enough that it deserves a
written reason, and if the answer turns out to be A, the reason is what stops the next review
re-opening it.
