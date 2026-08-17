---
title: One connection per cluster
scope: sidecar
status: Planned
---

# One connection per cluster

## Goal

Give each `Cluster` record its own pool entry in `kubeconn`, so a connection belongs to one cluster
rather than to a set of credentials several clusters may share.

## Depends on: the cluster prober

**There is no prober yet.** `clustersvc.GetConnection` and `RetryConnection` still
`panic("not implemented")` (`internal/clustersvc/service.go`), nothing calls `kubeconn.Acquire`, and
`sidecar/CLAUDE.md` says so outright. The prober is specified in
→ [spec: kubeconnection service](kubeconnection-service.md), §"First consumer: the cluster prober".

**Fold this into that spec's build rather than shipping after it.** The prober is the only thing
that will ever build a pool key, so writing it with fingerprint-only keying means writing its key
builder, its tests, and its acceptance criterion — "a second pass reuses the pooled connection; a
changed key gets a new one" — against a shape that this spec immediately rewrites. Write the
per-cluster key on day one. The ADR and the doc reversal below still land as their own commit,
since they are what a reader will need explaining.

## What changes

`kubeconn` keys entries on a string the caller supplies, and never interprets it. The prober would
otherwise pass `kubeconfig.fingerprint` alone, which describes credentials — so two kube-contexts
aimed at one server as one user land on one entry.

**It passes the cluster's id alongside the fingerprint instead:**

```go
// poolKey is the kubeconn entry key for one cluster's credentials. The fingerprint is
// what makes a credential rotation a different entry; the id is what keeps two clusters
// off one socket, however alike their credentials.
func poolKey(fingerprint string, id ClusterID) string {
	return fmt.Sprintf("%s/%d", fingerprint, id)
}
```

`ClusterID` is a `beehive.ObjectID` (`int64`), so `%d` is its whole encoding — the decimal form the
GraphQL scalar already uses.

**The invariant that makes it stick: a bare fingerprint is never a pool key.** The prober is the
only key builder in the process, and every `Acquire`/`ProbeNow` call goes through `poolKey`. Passing
a fingerprint straight through reintroduces sharing silently, with nothing failing — which is the
one regression worth a test of its own.

That is the whole code change. **`kubeconn` gets no behavioral change**: the key stays opaque to it,
and it still never learns what a cluster is. Sharing granularity was always the caller's decision;
this records which one the caller makes.

## Why

**Measurement — the load-bearing reason.** Anything measured at the socket (bytes, latency, error
rate, time of last byte) is attributable to exactly one cluster, with no apportionment.
→ [spec: per-cluster connection throughput](connection-throughput.md), which this unblocks.

The alternative — keep sharing and apportion measurements back to clusters — was specified and
rejected: it needed a second meter above the transport, in different units, a per-interval fold to
stay monotonic, and three edge cases, all to reconstruct a number that a per-cluster socket reports
outright.

**Demand decoupling — real, but secondary.** Two clusters on one entry share the probe that entry
runs, so `RetryConnection` on one is a `kick` that probes for both, and a failing cluster's backoff
ladder paces the other's re-probes. Both are mild; neither would justify the change alone.

Note what is *not* an argument here, so a later reader does not reverse the reversal on a bad one:
leases are refcounted (`lease.Release` nudges only at zero), so one cluster releasing while another
holds does nothing; and a rotation moving two shared clusters together is *correct* under shared
keying, since the credentials genuinely rotated for both.

## What it costs

Two kube-contexts pointing at one server as one user get two sockets and two probes instead of one.
The probes are 3 requests apiece per cadence, and only for leased clusters. A handful of extra
connections to one API server is unremarkable — `kubectl` opens one per command.

**It does not cost duplicate credential acquisition**, which is the objection worth answering
directly. client-go caches the exec `Authenticator` in a process-global map (`globalCache`, consulted
by `newAuthenticator` in `plugin/pkg/client/auth/exec/exec.go`) keyed by the exec config plus the
cluster, so identical exec settings against one server share one token and one helper invocation
however many connections exist. There is no extra `aws`/`gcloud` run, and no extra MFA prompt.

## The invariant this reverses

`kubeconn` states that credentials are the unit — "two contexts aimed at one server as one user are
one entry, one socket and one probe". That is a deliberate design claim, and "why isn't this pooled
by credentials?" is exactly what a future reader will ask, so the reversal gets an ADR rather than a
quiet edit.

The doc sites fall into three tiers. Sorting them is the point: without it, following through is a
judgment call per comment.

**Rewrite — the claim is now false.** Every statement about *who shares an entry*:

- `internal/kubeconn/service.go`, the package godoc's "Credentials are the unit throughout… Two
  contexts aimed at one server as one user are one entry, one socket and one probe".
- `internal/kubeconfig/restconfig.go`, `fingerprint`'s closing comment — "The context name is
  deliberately absent: two contexts pointing at one cluster with the same credentials share a
  connection." The *behaviour* it describes (the fingerprint ignores the context name) is unchanged
  and worth keeping; only the sharing conclusion goes.
- `sidecar/CLAUDE.md`'s **Kube connections** section, same claim.
- `docs/specs/kubeconnection-service.md`: the "Keying on credentials rather than on a cluster id
  means two records aimed at the same cluster share one connection" paragraph, and the build-order
  item and acceptance criterion that test it.

**Re-word — the premise is falsified, the conclusion survives.** `internal/kubeconn/service.go`, the
tuning const block: "The pool key describes credentials only, so two callers under one key with
different tuning would silently share whichever connection built first." *Callers*, not clusters —
the probe, a sync worker and a log tail all share one cluster's key, so the reason the package
stamps its own tuning is untouched. Only "describes credentials only" is wrong; it becomes something
like "the pool key is the caller's, and never describes tuning". Do not delete the rationale.

**Survives — loose shorthand.** "A set of credentials" as a synonym for an entry, in ~35 doc
comments across `service.go`, `lease.go`, `loop.go` and `probe.go` (`Result`, `ProbeNow`, `State`,
`entryFor`, `Lease`, …). An entry still *is* one set of credentials; it is no longer the only entry
those credentials could have. Leave them. Rewriting 35 comments to say "one set of credentials for
one cluster" would put the caller's policy into a package that must not know it.

The rule separating tier 1 from tier 3: **a comment that says what an entry holds is fine; one that
says who else is on it is not.**

## Build order

One commit, on top of the prober's.

1. The prober's key builder becomes `poolKey`; the ADR and the tier-1/tier-2 doc sites land with it.

Two tests, both in `clustersvc` against the fake `kubeconnService` (which records the keys it is
handed) — `kubeconn` needs none, since it is unchanged:

- Two clusters whose contexts resolve to identical credentials are acquired under **distinct** keys.
  This is the behaviour being reversed; nothing else would catch a later "optimization" folding them
  back together.
- Every key the prober builds carries the cluster id — i.e. no call passes a bare fingerprint. This
  is the invariant above, and it fails closed where a reviewer's eye would not.

## Not in this pass

- **Any behavioral change to `kubeconn`.** If this spec makes you edit anything but doc comments
  under `internal/kubeconn`, the key stopped being opaque and something is wrong.
- **Idle reclaim.** Per-cluster entries mean more of them, which makes reclaiming idle ones matter
  sooner — but it is the same sweep the kubeconnection spec already defers, not a new problem.

## Done when

Two kube-contexts pointing at one cluster with the same user hold two entries, two sockets, and
report their probes independently; `RetryConnection` on one leaves the other alone. The diff under
`internal/kubeconn` is doc comments only.

Delete this spec when it lands, leaving the ADR.
