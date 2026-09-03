---
title: A kubeconfig exec plugin waits for approval
scope: sidecar · frontend
status: Planned
---

# A kubeconfig `exec` plugin waits for approval

**Needs:** nothing. `observeKubeconfig` and its status block already carry one such
observation, `cluster.entry`; this follows its shape — mirror what the file says and leave
the verdict to the frontend, per
[ADR: status mirrors the kubeconfig](../adr/2026-09-03-status-mirrors-the-kubeconfig.md).
**Hands on:** nothing.

## Goal

Stop the app from executing programs named by a file, before the user has said yes to that program.

A kubeconfig context can name an `exec` credential plugin: a binary `clientcmd` runs to mint a
token. The sidecar imports **every** context as an enabled cluster (`clusters.go:788`), and the
connection probe dials every enabled cluster on startup and again on every kubeconfig change. So
writing a file into `~/.kube/` runs a program of the writer's choosing, repeatedly, for contexts
the user has never opened.

`kubectl` runs the same plugins, so the bar is not "never exec". The bar is **not before the user
has approved that program for that cluster**.

## Two decisions this rests on

**Approve on first use. Do not import these clusters disabled.** EKS, GKE and AKS all authenticate
through `exec` plugins, so defaulting them off means most cloud users open the app to an empty
context picker — the picker filters on `spec.enabled` (`src/lib/kube-config.tsx:52`), so a disabled
cluster is invisible, and so is any explanation attached to it. The import loop's own comment
already says this: a tracked context the user has to switch on before it appears "reads as a broken
import, not a default". So the cluster imports enabled and visible, and what waits is the **dial**.

**Approval names the credentials, not the cluster.** The threat is a file write, and a file write
can also change the `command` of a context the user approved last month. A yes/no flag on the
cluster record would let that through, which is the whole hole again for every cloud context the
user actually uses. So what the user approves is the credential block as it stood when they read
the prompt, and the gate re-arms the moment it differs.

There is already a value that names it. `kubeconfig.RESTConfig` returns a **fingerprint** beside
the config — a hash over everything that resolves the context's credentials, including the exec
plugin's command, arguments, environment and API version (`kubeconfig/restconfig.go`,
`fingerprint`). The connection probe already recomputes it on every run and rebuilds the connection
when it moves. Approval stores that fingerprint; nothing new is hashed.

## Where the gate lives: the probe, not the controller

The kubeconfig watch wakes the connection probe **directly** (`kubeconn`, `WakeAll(nameConnection)`),
in parallel with the cluster controller's own pass. A gate in the controller — observe the file,
decide, drop the lease — would race the probe re-reading the same change and dialling with the new
plugin before the lease is gone. The one place with no window is the probe itself, after it has
resolved the config it is about to use and before it builds a connection from it.

So the gate is in `connectionProbe.Run` (`kubeconn/probe.go:150`), and the controller's job is to
tell the probe what is approved, read the verdict back, and show it.

## What to build

### Sidecar

**1. The approval reaches the probe through the lease.** Add to `kubeconn.Lease`:

```go
// ApproveExec names the credential fingerprint whose exec plugin may run. Empty approves
// nothing. A changed value wakes the connection probe, so a fresh approval dials at once.
ApproveExec(fingerprint string)
```

Stored on the pool's per-context entry beside the claims; the probe reads it through its pass. The
cluster controller calls it every pass from `obj.Spec.ExecApproved`, just after `ensureLease` in
`reconcileConnection` (`clusters.go:650`) — idempotent, and cheap when unchanged.

**2. The gate.** In `connectionProbe.Run`, once `RESTConfig` has returned `cfg` and `fingerprint`
and before the rebuild arm:

```go
if cfg.ExecProvider != nil && fingerprint != approved {
    // Never build from this config: the transport runs the plugin on its first request,
    // and a connection built under the previous fingerprint ran a program this one
    // may not be. Retire it rather than keep it.
    next = connInfo{fingerprint: fingerprint}
    return supervisor.Suspend(ReasonExecNotApproved,
        fmt.Sprintf("context runs %q to authenticate; approval required", cfg.ExecProvider.Command))
}
```

A suspended probe is what the pool already does for a departed context, so the lease's `Conn`
returns `ErrNoConnection`, the caches pause on `ReasonNoConnection`, and nothing else in the process
can reach a transport for this context. `ReasonExecNotApproved` is a new `supervisor.Reason` beside
`ReasonContextNotFound`.

**3. Observe the plugin, for the prompt.** In `observeKubeconfig` (`clusters.go:806`), read the
user entry's exec block onto the user half of the status block, which today carries the entry name
alone:

```go
// ExecPlugin is the credential plugin the context would run, nil when it has none.
// Observed off the file, never resolved: knowing a plugin exists must not require
// running it.
ExecPlugin *ClusterExecPlugin
```

with `ClusterExecPlugin{Command string; Args []string; Fingerprint string}`. Command and args come
from `cfg.AuthInfos[kctx.AuthInfo].Exec`, both lookups guarded for nil. The fingerprint comes from
`c.kubeconfigSvc.RESTConfig(src.Context)` — already on the controller's interface
(`shared.go:72`) — taken in `Reconcile` beside the observation; on a resolve error the previous
value stands, since a file that will not resolve cannot be approved either and the probe reports
that on its own. The departed-context branch copies the previous block wholesale, so the value is
retained for free.

The schema field, on `ClusterStatusSourceKubeconfigUser` beside `name` — a projection of the
authInfo entry, never a mirror of it, since that entry holds the credentials:

```graphql
"The credential plugin this context would run; null when it has none. `fingerprint` is what
clusterExecApprovalSet takes: approving it approves exactly this command, these arguments and this
environment, and any change to them asks again."
execPlugin: ClusterExecPlugin
```

Exposing the command and arguments to the webview is fine — a path and flags, not a credential.
The exec block's `env` values are folded into the fingerprint but not shown.

**4. The approval on the spec.** `ExecApproved string` on `ClusterSpec` — the fingerprint, empty
when nothing is approved — and a mutation beside `clusterEnabledSet` (`schema.graphqls:908`):

```graphql
"Approves running this context's exec credential plugin, as identified by the fingerprint the
status block shows. Null revokes. Until approved the cluster is visible but never dialled."
clusterExecApprovalSet(id: ObjectID!, fingerprint: String): Cluster!
```

The caller passes the fingerprint it displayed, so what is approved is what the user read — not
whatever the file says at the moment the click lands. Implement it as `SetEnabled` is implemented
(`clusters.go:335`): one `updateSpec` call.

**5. Read the verdict into the conditions.** `reconcileConnection` folds the probe's state into a
`connectionFinding`; today a suspended probe reads as `PhaseUnreached` and reports
`ReasonProbeFailed`. Add an arm ahead of it:

```go
case observed.Connection.LastAttempt.Reason == kubeconn.ReasonExecNotApproved:
    finding.inactive = true
    finding.reason, finding.message = ReasonExecPluginNeedsApproval, observed.Connection.LastAttempt.Message
```

`inactive` is right: like a disabled cluster, this is a choice the user has not yet made, not a
server that failed to answer.

**A reason is not a condition, and the two conditions will disagree.** An inactive finding produces
both `Connected` and `Identified`, and `observeIdentified` hardcodes `ReasonInactive` for *any*
inactive finding (`clusters.go:690`) — so the record would carry
`Connected=False/ExecPluginNeedsApproval` beside `Identified=False/Inactive`. Pass `finding.reason`
through the inactive arm instead, so both conditions say the same thing. Step 7 keys on
`Connected=False` with reason `ExecPluginNeedsApproval` — say that exactly, in the frontend code
and in the test.

The reason must be its own: a disabled cluster already reports `Connected=False` / `ReasonInactive`
/ "cluster is disabled", and the UI has to tell the two apart.

**6. Record it on the timeline — from the controller, not the probe.** `kubeconn` is a probe
package under `clustersvc/internal/`; nothing in it touches beehive. Events are written by the
controllers, through `client.AddEvent` (`caches.go:1126`).

So the cluster controller writes it, in the same pass that has the observation and the finding —
exactly like `logDiscoveryVerdict`. Beehive **extends** a run when `(Category, Type, Reason)`
repeats rather than appending a row, so the controller writes every pass and the "no noise per
dial" property falls out of the store rather than out of a guard. One event, written while the
context has an exec plugin and the connection is up: **resolved credentials via exec plugin
`<command>`** — `clientcmd` runs the plugin lazily, when a token is first needed, so "resolved"
rather than "ran".

### Frontend

**7. The approval prompt.** In the context bar (`src/components/widgets/kube-context-bar.tsx`),
read the active cluster through `useActiveCluster()` — it carries the record's `id`, `conditions`
and status. When the `Connected` condition is false with reason `ExecPluginNeedsApproval`, render a
banner above the outlet:

> This context runs `<command> <args…>` to sign in. Approve to let that program run.
> **[Approve]**

The button calls `clusterExecApprovalSet(id, fingerprint)` with the fingerprint from the same
frame the banner rendered. The conditions are already selected in full (`src/lib/clusters.tsx:82`),
so the only addition to `ClustersWatchSubscription` is `execPlugin { command args fingerprint }`
under `status.source.kubeconfig`.

The picker needs no change: the cluster is enabled, so it is already listed.

## What this covers, and what it does not

- A changed command, argument, or plugin environment re-arms the gate, because the fingerprint
  moves. So does a rotated CA or a moved `proxy-url`, since the fingerprint covers the whole
  credential block; re-asking on those is a small price for one value that means "what you
  approved".
- A context deleted and re-added with the same block inherits its approval — the same program,
  approved once.
- `PATH` still decides which binary `command: aws` resolves to. That is `kubectl` parity, and the
  approval names the command as written; the spec does not try to pin the binary.
- Nothing here runs less often for a context the user has approved. The gate is about the first
  run, not the hundredth.

## Tests

In `kubeconn`, with the probe fixtures:

- A context with an exec block and no approval never builds a connection — asserted through the
  fixture, not by timing — and reports `ReasonExecNotApproved`.
- `ApproveExec` with the matching fingerprint arms the probe on the next run; with a stale one it
  does not.
- A kubeconfig change that moves the fingerprint under an approved context suspends the probe
  again and retires the connection it had.
- A context without an exec block is unaffected by any of it.

In `clustersvc`, with the existing kubeconfig fixtures:

- A context with an `exec` block observes command, args and fingerprint onto the status; one
  without observes nil.
- Such a cluster imports **enabled**, and appears in the cluster list.
- The probe's verdict surfaces as `Connected=False` / `ReasonExecPluginNeedsApproval`, with
  `Identified` carrying the same reason — distinct from `ReasonInactive`.
- `clusterExecApprovalSet` writes the spec, and the next pass hands the value to the lease.
- The first connected pass writes the event naming the command; a second pass **extends the same
  run** rather than adding a row.

In `src/`: a test that the banner renders for a cluster with the condition and not otherwise, and
that Approve calls the mutation with the frame's fingerprint.

## When it lands

Move *"A gesture before a kubeconfig `exec` plugin runs"* out of **Not built** in
[`docs/security-model.md`](../security-model.md) to **Enforced**, naming the `kubeconn` tests.
Note the gate in `sidecar/CLAUDE.md` where cluster import and the connection probe are described —
"every enabled cluster is dialled" gains an exception, and it needs to be stated where someone will
find it.
