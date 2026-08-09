---
title: Serve object bodies verbatim through a comparable RawJSON scalar; derive columns client-side
date: 2026-08-09
scope: cross-cutting
status: Accepted
---

# Serve object bodies verbatim through a comparable RawJSON scalar; derive columns client-side

## Context

The dashboard's generic objects table shows any synced kind. Beyond the universal identity
columns, useful cells (Ready, Status, …) are kind-specific and derived from the object body.
The sidecar already stores each object's full JSON in the per-cluster cache. The delta-watch
reducer (`cacheDeltaWatch[T]`) detects `Modified` frames by comparing projections, which
requires `T comparable` in Go.

## Decision

`ClusterDataObject` carries the typed universal identity **plus `rawJSON`** — the object's
full native body as a custom `JSON` GraphQL scalar bound to `cluster.RawJSON`
(`sidecar/internal/cluster/rawjson.go`): a Go `string` holding already-serialized JSON whose
`MarshalGQL` writes the stored bytes verbatim (empty → null). The string underlying type
keeps the projection struct `comparable`, so the body participates in `cacheDeltaWatch`'s
diff — which is what makes an in-place edit (a `resourceVersion`/spec change under a stable
`uid`) surface as `Modified` rather than nothing. It autobinds with no field resolver: the
watch reads the body for every row anyway, so gating the marshal would defer nothing.

Bodies are sanitized at **write** time (`objectsync`): `metadata.managedFields` and the
kubectl last-applied annotation stripped (roughly half the bytes, nothing reads them), Secret
values redacted. The read path is a pure pass-through. Kind-specific columns are derived
**client-side** (a per-kind registry in the webview, typed as `unknown` and cast); the
sidecar computes no cell values.

## Alternatives considered

**Server-computed per-kind columns.** Rejected: every new kind or column means a sidecar
change plus schema change plus regen on both sides; client-side derivation over the verbatim
body means a frontend-only change. (The cache's materialized status columns exist for SQL
queryability, not for the dashboard.)

**`map[string]any` / structured JSON scalar.** Rejected: not `comparable`, so the diff that
produces `Modified` frames stops compiling; deep-equal comparison would be both slower and a
second code path.

**Omitting the body and re-fetching on demand.** Rejected: the table needs derived cells for
every visible row, so it would fetch per row anyway — and edits under a stable uid would be
invisible to the watch.

## Consequences

Stored and wire bytes stay ~half the size, secrets never land on disk in plaintext, and
frontend column work needs no sidecar involvement. The obligations: sanitize stays at write
time (readers assume it); `RawJSON` stays a string-underlying comparable type; consumers
treat the body as `unknown`, never trusting it to match a generated type.
