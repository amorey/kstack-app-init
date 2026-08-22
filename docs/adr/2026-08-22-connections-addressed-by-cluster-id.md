---
title: Address every Kubernetes connection by ClusterID
date: 2026-08-22
scope: sidecar
status: Accepted
---

# Address every Kubernetes connection by ClusterID

## Context

`internal/kubeconn` is a worked-out connection pool: one entry per credential *key*, built on
first use, kept validated by a probe, handed out only through a `Lease`. It reads no kubeconfig
and knows nothing about a cluster — the caller computes the key.

It sat in `internal/` as a peer of the cluster service, on the reading that it is a transport
leaf several subsystems would draw from directly: log tail, exec, port-forward, the kube-context
picker's metadata, the `SelfSubjectRulesReview` behind `ClusterPermissions`. The composition root
built it and drove its lifecycle; nothing acquired a lease.

That reading did not survive being checked. Every one of those consumers knows *which cluster* it
means, and none of them can turn that into credentials on its own: the `ClusterID` → source →
context name → `rest.Config` chain lives entirely inside `clustersvc`. Worse, dialing needs policy
that only `clustersvc` holds — whether the cluster is enabled, whether its record is tombstoned,
and whether the server answering is still the one `ClusterActiveUID` identifies. A tail streaming
from a server the cluster record no longer claims is a bug the pool cannot even describe.

We could not construct a case for connecting to a cluster without addressing it by `ClusterID`.
The nearest was "validate credentials before committing them", which the record shape already
answers: `ensureKubeconfigClusters` derives a record per kubeconfig context, so the record exists
before the dial does, and a manual-add flow can create a disabled one first. In-cluster /
service-account mode would be a real exception, but that is kubetail's cluster-api, not this
desktop app.

## Decision

Connections are obtained by `ClusterID`, from `clustersvc`. The pool lives behind that boundary at
`internal/clustersvc/internal/kubeconn`, alongside `kubeidentity`, and is unimportable from
anywhere else. `clustersvc.New` constructs it and carries it in the service's own `lifecycle.Part`
slice, ahead of beehive, so a connection outlives every reconcile pass that could still be dialing
on it. The composition root no longer knows the pool exists.

The pool still keys on credentials, not on clusters: two kube-contexts aimed at one server as one
user are one socket and one probe. Only the *address a caller uses* is a cluster.

`internal/kubeconn` stays in the tree, built by nothing, as the worked-out implementation the new
package draws from as it fills in.

## Alternatives considered

**Leave it in `internal/` and let subsystems dial directly.** This is what we had. It requires a
credential key or a context name as the address, and no consumer has one — they would each
re-derive the `ClusterID` → credentials chain, and each would have to remember the enablement,
tombstone, and server-UID checks that make a dial legitimate. Every place that forgot one would be
a bug the types could not catch.

**Leave it in `internal/`, but make `clustersvc` the only caller.** Consistent, and it preserves
the composition root's explicit start ordering. Rejected because the ordering guarantee it
preserves is a comment nobody may reorder, whereas ownership makes it structural — and because
`internal/` placement would then describe nothing true about the package except that one importer
happens to be allowed to bypass the boundary.

**Bury it and drop the credential key, one entry per cluster.** Simplest to explain, and wrong: it
would dial and probe a server once per kube-context pointing at it, which is exactly the waste the
key exists to remove.

## Consequences

The composition root gets smaller and the pool's lifecycle is enforced by position in
`clustersvc`'s slice rather than by a comment in `app.go`.

`kubeconn` and `kubeidentity` become siblings, which exposes how much they overlap:
`kubeconn.Identity` is `kubeidentity.Identity` plus `UIDErr`, and both carry a `State` and a
conflate-keyed `Subscribe` differing only in key — credentials versus context name.
`kubeidentity`'s probe is unwritten, so the two are likely to collapse into one package with two
views rather than staying separate.

The obligation the new location creates: the pool's unit is credentials, shared *across* clusters,
and nothing in the vocabulary of a cluster record says so. The package doc has to lead with it, or
a reader will assume one entry per cluster and reason wrongly about probe counts and socket reuse.

Anything genuinely needing a connection without a `ClusterID` now has to go through `clustersvc`
anyway, or get a record first.

## Revisit when

The sidecar grows an in-cluster / service-account mode, or any dial whose subject is not a tracked
cluster — a bare credential check in an add-cluster flow that we decide should not create a record
first.
