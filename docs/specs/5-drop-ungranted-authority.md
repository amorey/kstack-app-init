---
title: Drop the two grants with no consumer
scope: host · sidecar · frontend
status: Planned
---

# Drop the two grants with no consumer

**Needs:** nothing. **Hands on:** nothing.

## Goal

Stop granting authority ahead of a feature that uses it. Two things are reachable today that
nothing needs:

- **`opener:default`** in `src-tauri/capabilities/default.json` lets any script in the webview ask
  the host to open a URL or a file. Nothing in `src/` calls it, and `src/` has no external link
  for the plugin's own click handler to route. The tray's "Account" item opens a URL through
  `OpenerExt` in `src-tauri/src/tray/mod.rs`, which is Rust-side and needs no webview capability.
- **`chatStream`** is in the shipped GraphQL schema, and its resolver is
  `panic("not implemented: ChatStream")` (`sidecar/graph/schema.resolvers.go:318`). The panic is
  recovered per request, so the cost today is a failed subscription — but it is a live entry point
  for an assistant, and an assistant is the first path by which cluster data would leave the
  machine.

## What to build

### Drop the opener grant

Remove the `"opener:default"` line from the `permissions` array in
`src-tauri/capabilities/default.json`, and remove `@tauri-apps/plugin-opener` from `package.json`
— it is installed and imported nowhere. Leave `tauri_plugin_opener::init()` in `lib.rs` and the
crate in `Cargo.toml`; the tray still uses them from Rust.

Check it still works: run the app, use the tray's account item, confirm the browser opens.

**When a webview link is needed later** — a docs link, a cluster's dashboard URL — the grant comes
back scoped, as `opener:allow-open-url` with its `https` scope, rather than `opener:default`. The
plugin's init script routes `target="_blank"` clicks through that command, so an external link
without the grant is a silent no-op.

### Drop chatStream

Four edits, in this order:

1. **Schema** (`sidecar/graph/schema.graphqls`): delete the `chatStream` field under the
   `# --- Chat ---` heading in `Subscription`, and the `ChatInput` and `ChatChunk` types under the
   same heading near line 841. Delete the sentence in the `Subscription` type's comment that says
   "`chatStream` alone completes" — with it gone, every subscription is endless.
2. **Resolver**: regenerate with gqlgen, which drops `ChatStream` from `generated.go` and the
   models. Then delete the now-orphaned `ChatStream` method from `schema.resolvers.go`.
   `sidecar/graph/generated*.go` is generated — never hand-edit it.
3. **Webview** (`src/routes/chat.tsx`): delete the `ChatStreamSubscription` document and the
   `useWatchSubscription` call, along with the `pending` / `streamed` / `finishedRef` state that
   only exists to drive it. Keep the route and its layout. Disable the composer and render one
   line where the empty state is today: `Chat isn't available yet.` The point is that the route
   stays deep-linkable and the sidebar's mode switch keeps working.
4. Run `pnpm codegen`, then `pnpm lint` and `pnpm test --run`.

## Tests

- `src/routes/chat.test.tsx` if one exists — otherwise none is needed; the route renders a static
  message.
- The sidecar's schema tests will fail if step 2 is done out of order. That is the check.

## When it lands

Move the row *"No authority granted ahead of a consumer"* in
[`docs/security-model.md`](../security-model.md) out of **Not built** to **Held by review** — a
capability file and a schema are reviewed, not tested. Note in the root `CLAUDE.md`'s routing
section that the chat route is a placeholder. When the assistant feature is built for real, re-add
each grant scoped to what it actually needs — and write the egress question down before the first
token leaves.
