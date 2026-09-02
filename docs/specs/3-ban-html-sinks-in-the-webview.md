---
title: A lint rule bans HTML sinks in the webview
scope: frontend
status: Planned
---

# A lint rule bans HTML sinks in the webview

**Needs:** nothing. **Hands on:** nothing.

## Goal

Turn "we never render cluster data as HTML" from a habit into a build failure.

Everything the webview displays — object names, labels, annotations, event messages — is text
chosen by whoever controls the cluster. `src/` has no HTML sink today: no `innerHTML`, no
`dangerouslySetInnerHTML`, no `eval`. Nothing stops the next change adding one, and if one lands,
a hostile cluster gets script execution in a page that holds the whole cluster surface.

## What to build

One rule block in `eslint.config.ts`, added to `customRulesConfig` (or as its own named config
object right after it, which reads better since it needs a `files` scope):

```ts
{
  name: 'custom/no-html-sinks',
  files: ['src/**/*.{ts,tsx}'],
  rules: {
    'no-restricted-syntax': [
      'error',
      {
        selector: "JSXAttribute[name.name='dangerouslySetInnerHTML']",
        message: 'Cluster data is attacker-controlled text; render it as text, never as HTML.',
      },
      {
        selector: "MemberExpression[property.name=/^(innerHTML|outerHTML)$/]",
        message: 'Cluster data is attacker-controlled text; use textContent.',
      },
      {
        selector: "CallExpression[callee.name='eval']",
        message: 'No eval in the webview.',
      },
      {
        selector: "MemberExpression[property.name='insertAdjacentHTML']",
        message: 'Cluster data is attacker-controlled text; render it as text, never as HTML.',
      },
    ],
  },
}
```

Two things to get right:

- **Scope it to `src/`.** `src/gql/` is already ignored globally, and the rule has no business in
  config files or `src-tauri/`.
- **`no-restricted-syntax` is already used by airbnb's base config.** Listing it again in a later
  config object *replaces* airbnb's entries rather than adding to them — ESLint's flat config does
  not merge rule options. Check `pnpm lint` output before and after: if airbnb's restrictions
  disappear, copy them into this block alongside the four selectors above.

Then run `pnpm lint` and fix anything it catches. It should catch nothing.

## Tests

No unit test — the lint run is the test. Verify by temporarily adding
`<div dangerouslySetInnerHTML={{ __html: name }} />` to a component, confirming `pnpm lint` fails,
and removing it.

## When it lands

In [`docs/security-model.md`](../security-model.md), move the row *"The webview renders no HTML from
cluster data"* to **Enforced**, naming the ESLint rule instead of the TODO link. The
`## Security invariants` bullet in the root `CLAUDE.md` says this property is "kept, not a
coincidence" — say instead that lint enforces it.
