---
title: The host sends only the operations the app ships
scope: host · frontend · build
status: Planned
---

# The host sends only the operations the app ships

**Needs:** [3-ban-html-sinks-in-the-webview.md](3-ban-html-sinks-in-the-webview.md) is the cheap half
of the same problem and should land first. **Hands on:** nothing.

## Goal

Make script execution in the webview stop being the same thing as full cluster access.

`graphql_query` and `graphql_subscribe` forward whatever operation string the page hands them. The
sidecar then serves it: every query, every mutation, the whole schema. So a compromised dependency
in the webview can read every mirrored object and delete every cluster record without going near
the kubeconfig.

The app ships a fixed, known set of operations, and `src/gql/` is generated from exactly that set.

## The design: the page sends a hash, the host sends the document

**The page sends only a hash. The host looks the hash up in a generated table and forwards the
document from the table.** An operation not in the table has no document, so nothing is sent.

Substitution is the whole point of the shape. The obvious alternative — hash the incoming query
text and compare — does not work here, for two reasons worth stating so nobody rediscovers them:

- **The string the page would send is not the string codegen saw.** urql's `cacheExchange` runs
  `formatDocument` on every query and mutation, which injects `__typename` selections. A hash of
  the source text never matches what arrives.
- **Two normalizers that must agree byte-for-byte across two languages** is the part most likely to
  fail silently, and it would fail closed on legitimate traffic — every operation rejected, in
  release builds only.

Substitution needs neither. There is no normalizer anywhere in this design; if you find yourself
writing one, the design has drifted.

## What to build

**1. Generate the table, with `__typename` already in it.** Turn on graphql-codegen's persisted
documents in `codegen.ts`, and apply the client preset's own typename transform ahead of it:

```ts
import { addTypenameSelectionDocumentTransform } from '@graphql-codegen/client-preset';

generates: {
  'src/gql/': {
    preset: 'client',
    documentTransforms: [addTypenameSelectionDocumentTransform],
    presetConfig: {
      persistedDocuments: { mode: 'embedHashInDocument', hashPropertyName: 'hash' },
    },
    config: { /* as today */ },
  },
},
```

Use `embedHashInDocument`, not `replaceDocumentWithHash`: urql's cache needs the parsed document.

**The transform is what keeps the cache working.** The stored document is the text the sidecar
executes, so one without `__typename` selections yields responses without `__typename` — and urql's
document cache reads exactly those to decide which cached queries a mutation invalidates. It would
quietly stop invalidating. The preset's transform exists for this: it adds `__typename` to every
selection set except a subscription's root (verified in the installed `client-preset` 6.0.0,
`esm/add-typename-selection-document-transform.js`). The preset runs `documentTransforms` before
it hashes and prints each document, so the generated `graphql.ts`, the embedded hash and the
`persisted-documents.json` entry describe the same document (`esm/index.js:99-105`, the
`onExecutableDocumentNode` hook) — confirm that still holds on the installed version, since the
whole scheme rests on it.

**Budget for the churn.** The transform changes `graphql.ts`, not only the persisted map: every
operation's generated TS type gains `__typename` fields, so every value built against those types
has to carry them. That means the frame fixtures in `clusters.test.tsx`, the sync-health tests and
the cached-data tests. Mechanical, but it is the largest single chunk of work in this item — do it
first, not last.

The map lands at `src/gql/persisted-documents.json`, a path the Rust build can `include_str!`.

**2. Send the hash and nothing else.** The `fetch` adapter in `src/lib/graphql/invoke-fetch.ts`
never sees the operation — urql hands it a serialized body — so the hash has to be put on the
operation upstream, by an exchange. `@urql/exchange-persisted` is that exchange, and configured
this way it is exactly the contract:

```ts
persistedExchange({
  // The hash codegen embedded, never one computed here: there is no normalizer.
  generateHash: async (_query, document) => {
    const hash = (document as { __meta__?: { hash?: string } }).__meta__?.hash;
    if (!hash) throw new Error('operation is not in the persisted set');
    return hash;
  },
  // Never fall back to sending the document text.
  enforcePersistedQueries: true,
  enableForMutation: true,
  enableForSubscriptions: true,
})
```

placed after `cacheExchange` and before `fetchExchange` / the subscription exchange in
`client.ts`. With `extensions.persistedQuery` set and no miss, `@urql/core` omits `query` from the
body it builds (`makeFetchBody`, `urql-core-chunk.mjs:369`, verified on the installed 6.0.1) — so
the page sends `{ variables, extensions: { persistedQuery: { sha256Hash } } }` and no document at
all. `invoke-fetch.ts` forwards that envelope unchanged. `subscribe-exchange.ts` passes
`request.extensions` to the host where it passes `request.query` today.

`__meta__` survives `cacheExchange`: `formatDocument` rebuilds the node as `{...node, definitions}`,
and a spread copies enumerable own properties — `__key` is the only thing it loses, which is why
the code re-attaches it explicitly and non-enumerably (`urql-core.mjs:91-102`). One trap in
`keyDocument`, which runs first: it returns the cached parsed AST for an operation it has seen as a
**string**, and that AST has no `__meta__`. Every document here comes from `graphql()`, so it does
not arise — and the `generateHash` above throws if it ever does, so the failure is loud and in the
page rather than a silent forward. Confirm the option names against the installed
`@urql/exchange-persisted` when adding it.

**3. Take the hash at the boundary.** Both commands read it out of the envelope:

- `graphql_query(state, body: String)` keeps its signature — `body` is the **whole JSON envelope**
  (`commands.rs:69`). The host parses it as a `serde_json::Value`, reads
  `extensions.persistedQuery.sha256Hash`, looks it up, sets `query` to the table's document,
  removes `extensions.persistedQuery`, and forwards the rest — `variables`, `operationName` —
  untouched. Whatever `query` the envelope carried is overwritten, never merged.
- `graphql_subscribe(state, webview, extensions: serde_json::Value, variables, channel)` — the
  `query` argument becomes `extensions`; the host derives the document the same way.

An unknown or missing hash returns an error and logs at `warn` with the hash — never the variables,
which is the same rule the sidecar's error presenter follows.

The table loads once at startup into a `HashMap<String, String>`.

**4. Keep the dev loop working.** `pnpm codegen:watch` must regenerate the map, and a query change
must not force a Rust rebuild. So: debug builds read the JSON from disk on each call and fall back
to forwarding the page's own document with a warning if the file is missing; release builds
`include_str!` it and have no fallback, under `#[cfg(debug_assertions)]` so the two cannot be
confused. **Prove this before building the rest** — the dev ergonomics are the part of this item
that has not been tested, and if `codegen:watch` turns out to fight it, that finding is worth more
than the implementation.

## What this is not

**Not a cost limit.** An allowlisted query can still ask for something expensive, and the
variables are still the page's. It makes a cost limit a smaller problem; it is not one.

**Not a defence against the host.** Anything running inside the host process can call the sidecar
directly. This is a boundary between the *page* and the host, which is the boundary a dependency
compromise crosses.

## Tests

- **Rust:** an unknown hash is rejected; a known one forwards the table's document, whatever
  `query` the envelope carried; the same hash with different variables passes; a malformed envelope
  is rejected rather than forwarded.
- **Frontend:** a test that the body the fetch adapter receives for a generated query carries the
  hash and no `query`, and one that a subscription's bridge call carries `extensions` — the two
  take different paths to the bridge.
- **Frontend:** a test that a mutation still invalidates a cached query, which is what pins the
  document transform.
- **Codegen:** a test that a document in `persisted-documents.json` contains `__typename` — the
  cheapest guard against the transform silently not reaching that output.
- **CI:** a step asserting `pnpm codegen` leaves the tree clean, so an operation added without
  regenerating fails the build rather than the app.

## When it lands

Move *"The host forwards only operations the app ships"* out of **Not built** in
[`docs/security-model.md`](../security-model.md) to **Enforced**, and rewrite the first of the "Two
facts that shape everything": the host no longer forwards whatever the page asks for. Update the
same claim in the root `CLAUDE.md` — the `## Security invariants` first bullet and the GraphQL
transport section both say the operation is forwarded unexamined.
