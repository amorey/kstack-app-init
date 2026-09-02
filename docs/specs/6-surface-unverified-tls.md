---
title: An unverified cluster connection says so
scope: sidecar · frontend
status: Planned
---

# An unverified cluster connection says so

**Needs:** nothing. **Hands on:** [spec 10](10-approve-exec-credential-plugins.md) adds a second
observation to the same function and the same status block — build this one first, and 10 follows
its shape.

## Goal

Show the user when a cluster is being reached without a verified TLS connection.

A kubeconfig cluster entry can carry `insecure-skip-tls-verify: true`, or a plain `http://` server
URL. `clientcmd` honours both, the sidecar dials, and the connection looks exactly like a verified
one everywhere in the UI. Matching `kubectl`'s behaviour is right; not showing it is the gap.

## What to build

**1. Read it where the kubeconfig is observed.** `observeKubeconfig`
(`sidecar/internal/clustersvc/clusters.go:806`) already holds the `*api.Config` and the context, and
already caches `Cluster`, `User`, `IsPresent` and `IsDefault` onto the status block. The
present-context branch returns a fresh `ClusterStatusSourceKubeconfig` literal; add one more field
to it, read off the cluster entry the context names:

```go
// nil when the context names a cluster entry the file does not define.
c := cfg.Clusters[kctx.Cluster]
// ... TLSUnverified: c != nil && (c.InsecureSkipTLSVerify || strings.HasPrefix(strings.ToLower(c.Server), "http://")),
```

**Not from the probe.** Both are static facts the file states, so they are known the moment the
kubeconfig is read. Sourcing them from the resolved `rest.Config` instead would leave the warning
false until a first successful probe — exactly the moment you would want it shown. It also follows
the type's own rule: `ClusterStatusSourceKubeconfig` is what the kubeconfig says, cached at observe
time, while probe findings live on `ClusterServer`.

Follow the type's cache-on-absence behaviour: when the context is gone, the field keeps its previous
value along with the rest of the block.

**2. Add one schema field.** In `sidecar/graph/schema.graphqls`, on
`ClusterStatusSourceKubeconfig` (line 333):

```graphql
"True when the server's certificate is not checked: the cluster entry sets insecure-skip-tls-verify, or its server URL is plain http."
tlsUnverified: Boolean!
```

Regenerate with gqlgen, then run `pnpm codegen` in the webview.

**3. Select it and carry it to the bar.** The context bar has no GraphQL document of its own — it
renders `active` from `useActiveKubeContext`, whose records are the `KubeContextInfo` shape
`src/lib/kube-config.tsx` derives from `useClusters`. Three edits, one per hop:

- Add `tlsUnverified` to `ClustersWatchSubscription` in `src/lib/clusters.tsx:52`, under
  `status.source.kubeconfig`.
- Add `tlsUnverified: boolean` to `KubeContextInfo` and set it in the `contexts` map in
  `kube-config.tsx`.
- Nothing in `active-kube-context.tsx` changes; it passes the record through.

**4. Show it.** In `src/components/widgets/kube-context-bar.tsx`, beside the cluster metadata,
render a small warning badge when the field is true — text `Unverified TLS`, with a `title`
saying the server's certificate is not checked. There is no badge primitive in the bar today; make
it a styled `<span>` the way `TypeBadge` in `events-table.tsx` is, not a new component file.

## Tests

- **Sidecar:** a test on `observeKubeconfig` asserting the field is true for a context whose
  cluster entry sets `insecure-skip-tls-verify`, true for a plain-`http` server, false otherwise,
  and that it survives the context leaving the kubeconfig — that function's tests already cover
  the other cached fields.
- **Frontend:** a test in `kube-context-bar.test.tsx` pushing a frame with the flag set and
  asserting the badge renders, and one asserting it does not otherwise.

## When it lands

Move the row *"An unverified cluster connection is visible in the UI"* in
[`docs/security-model.md`](../security-model.md) out of **Not built** to **Enforced**, naming the
two tests.
