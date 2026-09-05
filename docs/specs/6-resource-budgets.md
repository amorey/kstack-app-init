---
title: Resource budgets
scope: sidecar, src-tauri
status: Planned
---

# Resource budgets

**Needs:** nothing. **Hands on:** nothing. Closes R-05, and the half of S-9 that declining an
allowlist did not settle.

## Goal

Put a ceiling on the work one request, one subscription, one cluster and one disk can cause.

We decided not to allowlist GraphQL operations — the app converges on the whole schema, so the cap
would not hold a line ([no GraphQL operation allowlist](../adr/2026-09-03-no-graphql-operation-allowlist.md)).
That decision was about *which* operations are allowed. It said nothing about *how much* one is
allowed to cost, and today the answer is: as much as it asks for.

## What is true now

The outer bounds exist and are real, but they all bound the same thing — bytes and time on one HTTP
request:

- `main.go`: a 64 MiB `MaxBytesHandler`, a 5s header timeout, a 60s idle timeout.
- The host's query client bounds its own response at 64 MiB with a 30s timeout.
- `kubeconn`: every non-watch Kubernetes request carries a 2-minute idle-read bound.
- `kubestore`: each cache file has a 2 GiB ceiling, judged by the janitor each sweep.
- Cloud SSE frames have a 1 MiB aggregate limit.

What has no bound:

- **Query shape.** gqlgen is built with `handler.New` plus three transports and one extension
  (`graph/server.go`); no complexity limit, no depth limit. A deeply nested query over the join
  fields costs whatever it costs.
- **Concurrency.** Any number of operations can be in flight; any number of subscriptions can be
  open. The host's registry (`graphql/subscribe.rs`) is a `HashMap` with no cap, and the sidecar
  counts nothing.
- **Kind fan-out.** A cache arms one worker per discovered kind, and the code's own comment says a
  cluster can have "hundreds of kinds". Each holds a watch.
- **Object size and decompression.** A single Kubernetes object body is bounded only by what the
  API server sends, and bodies are compressed into the cache — a hostile cluster picks both sides
  of that ratio.
- **Total disk.** The 2 GiB ceiling is *per cache*. Ten clusters is 20 GiB, plus `app.db`, plus
  WAL files, with nothing measuring the sum.

## Design

Four budgets. They are independent and land in this order, cheapest first.

### 1. Query cost

**Three mechanisms, and only one of them ships.** gqlgen 0.17.95 has
`extension.FixedComplexityLimit` and `Server.SetParserTokenLimit`. It has **no depth guard** — a
depth limit is a gqlparser validation rule we write, registered through `SetValidationRulesFn`.
`max_introspection_depth` in gqlparser's own rules package is the precedent to copy.

**What a fixed complexity limit actually buys us here.** `Config` declares no per-field
`Complexity` functions, so every field costs 1 and the limit bounds a query's *shape*, not its
fan-out: a list field returning ten thousand rows costs the same as one returning none. If fan-out
is the worry — and on a whole-fleet watch it is — the real work is writing those functions for the
list fields, and **this stops being the cheap step**. Decide which of the two problems is being
solved before starting; the ordering of this spec assumes shape, and moves if the answer is
fan-out.

**Where the limit's number comes from.** It should be the app's own worst operation with headroom,
computed rather than chosen, so it does not drift the first time a view adds a field. But the check
belongs on the **JavaScript side**, not in a Go test: the sidecar deliberately lives outside
`package.json` and must build alone, so a Go test globbing `src/**` would couple them. Codegen
already parses every document — that is where the number is derived and asserted, and the Go side
carries only the constant.

### 2. Concurrency and subscription quota

- **A global subscription cap in the sidecar.** Global, not per-connection: there is one peer over
  one h2c connection, so those are the same number and the honest name is the global one. A count
  on the GraphQL server, refusing past the cap with a plain error. It is generous — the dashboard
  opens one per watched collection — and exists so a loop cannot open ten thousand.
- **The host caps its registry too**, at the same order of magnitude. Two caps rather than one
  because they fail differently: the host's protects the host's memory and its channel table, the
  sidecar's protects the sidecar's goroutines.
- **A ceiling on in-flight non-subscription operations**, queueing past it rather than rejecting —
  a query that waits is fine; a thousand concurrent ones are not.

### 3. Kind fan-out

Cap the number of kind workers a single cache runs concurrently. The supervisor already owns
worker admission (`enterKindRun`), so this is a gate there, not a new mechanism. Kinds past the cap
wait; they do not fail. The user-visible effect is that a huge cluster syncs its kinds in waves,
which is better than opening four hundred watches at once.

**Bound one object, too.** A body over a fixed size is refused at the store boundary and recorded
as a sync failure for that kind, rather than written. Decompression is bounded by the same number
on the way out.

### 4. A disk budget across all caches

The per-cache ceiling stays exactly as it is — it is what
[bound the cache by total size](../adr/2026-09-03-bound-the-cache-by-total-size.md) decided, and its
stopped-cache behaviour is settled. What is added is a **total**, measured by the same janitor
sweep across every open cache plus `app.db`, and reported like a per-cache verdict.

When the total is over, the pass stops the **largest** cache first, which is the one whose stopping
buys the most and the one the user is most likely to recognise. Choosing by size rather than by
last-use keeps the rule explainable in a sentence, which matters for something that stops a sync.

**Stopping is not eviction, and that is the trap this design has to answer.** A stopped cache keeps
its file — [bound the cache by total size](../adr/2026-09-03-bound-the-cache-by-total-size.md)
decided that deliberately, and [a stopped cache is held by its
record](../adr/2026-09-03-a-stopped-cache-is-held-by-its-record.md) keeps it stopped until the user
clears it. So a total budget that only stops caches never lowers the total: each sweep stops the
next-largest, and the fixed point is every cache stopped and the app permanently dead.

**So the total budget needs a way down, and this spec does not get to leave it open.** Either it
only ever stops the single largest cache and reports the total, leaving the user to clear one — in
which case it is a *notification* with one safety brake, and should say so — or it evicts, which
contradicts a standing ADR and therefore needs a new one first. Settle that before writing the
sweep. Whichever it is, the rule the user sees must be one sentence, and the app must have a path
back to working that does not involve deleting files by hand.

## Rules

- **Every limit has a test that computes it or crosses it.** A constant nobody exercises is a
  number, not a budget.
- **Refuse cheaply, before the work.** A cap enforced after allocation is a slower way to run out.
- **A budget the user hits must say so.** The cache stop already publishes a reason; the others need
  one too — a silent cap reads as a bug.

## Build order

Four commits, in the numbered order above — but the order assumes 1 is the cheap one, which holds
only if shape rather than fan-out is the problem. Each step is independent: 1 is an extension, a
validation rule and a codegen-side check; 2 is two counters; 3 touches `kubesync`; 4 touches the
janitor and the cache pass, and does not start until its recovery question is answered.

## Not in this pass

- **A memory budget.** Go's heap is not something a few counters bound honestly, and the real
  driver — object bodies in flight — is bounded by the per-object and fan-out caps above.
- **Rate limiting the webview.** The webview is trusted (H-1); a cap there would be a bug detector,
  not a control. The caps above happen to catch a runaway loop anyway.
- **Per-cluster network budgets.** Throughput is measured by
  [connection throughput](connection-throughput.md); turning a meter into a limiter is a separate
  decision.
- **Retention.** Ageing cached data out is its own open item, and the ceiling is what bounds it
  today.

## When it lands

- `security-model.md`'s cache-ceiling row gains a companion: *A total disk budget across caches and
  `app.db`*, plus rows for the query-cost and subscription caps, each **Enforced** by its test.
- The **R-05 / S-9** bullet leaves `TODO.md`.
- Delete this spec.

## Done when

A hand-written query nested past the limit is refused with a clear error, and every operation the
app ships still runs. A script opening subscriptions in a loop is refused at the cap instead of
growing the process. A cluster with several hundred kinds syncs without opening all of them at once.
Filling the disk with ten caches stops the largest one and says why.
