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

One named config object in `eslint.config.ts`, right after `customRulesConfig` (it needs a `files`
scope, which the shared rules block does not have):

```ts
{
  name: 'custom/no-html-sinks',
  files: ['src/**/*.{ts,tsx}'],
  rules: {
    // airbnb ships this as a warning; a warning does not fail `pnpm lint`.
    'react/no-danger': 'error',
    'no-restricted-syntax': [
      'error',
      // airbnb's four, restated: a later config object REPLACES this rule's options
      // rather than merging with them, so omitting these silently drops them.
      { selector: 'ForInStatement', message: 'Use Object.{keys,values,entries} and iterate.' },
      { selector: 'ForOfStatement', message: 'Use array methods rather than for-of.' },
      { selector: 'LabeledStatement', message: 'Labels obscure control flow.' },
      { selector: 'WithStatement', message: '`with` is disallowed in strict mode.' },
      // The DOM's HTML sinks, by dot and by bracket.
      {
        selector: "MemberExpression[property.name=/^(innerHTML|outerHTML|insertAdjacentHTML|createContextualFragment|write|writeln)$/]",
        message: 'Cluster data is attacker-controlled text; render it as text, never as HTML.',
      },
      {
        selector: "MemberExpression[computed=true][property.value=/^(innerHTML|outerHTML|insertAdjacentHTML|createContextualFragment|write|writeln)$/]",
        message: 'Cluster data is attacker-controlled text; render it as text, never as HTML.',
      },
      { selector: "JSXAttribute[name.name='srcdoc']", message: 'No inline frames from data.' },
      // Belt and braces: the CSP already refuses these at runtime.
      { selector: "CallExpression[callee.name='eval']", message: 'No eval in the webview.' },
      { selector: "NewExpression[callee.name='Function']", message: 'No eval in the webview.' },
    ],
  },
}
```

`react/no-danger` is the rule made for `dangerouslySetInnerHTML` — it also catches the prop on a
`React.createElement` call, which a JSX selector would not. `react/jsx-no-script-url` (`javascript:`
hrefs) is already an error in airbnb's React set and needs nothing here.

Two things to get right:

- **Scope it to `src/`.** `src/gql/` is already ignored globally, and the rule has no business in
  config files or `src-tauri/`.
- **The four airbnb entries above are not optional.** They are exactly what the installed config
  resolves to today (`eslint-config-airbnb-extended` 3.1.0, its style rules): `ForInStatement`,
  `ForOfStatement`, `LabeledStatement`, `WithStatement`. ESLint's flat config replaces a rule's
  options rather than merging them, so a block that lists only the HTML sinks silently turns
  airbnb's four off.

Then run `pnpm lint` and fix anything it catches. It should catch nothing.

## What this does not cover

A sink reached through a variable — `const k = 'innerHTML'; el[k] = s` — or through a dependency's
API. The first is caught by review, since nobody writes it by accident; the second is what the CSP
and [spec 11](11-allowlist-graphql-operations.md) are for.

## Tests

No unit test — the lint run is the test. Verify by temporarily adding
`<div dangerouslySetInnerHTML={{ __html: name }} />` and `el.innerHTML = name` to a component,
confirming `pnpm lint` fails on both, and removing them.

## When it lands

In [`docs/security-model.md`](../security-model.md), move the row *"The webview renders no HTML from
cluster data"* to **Enforced**, naming the ESLint config object instead of this spec. The
`## Security invariants` bullet in the root `CLAUDE.md` says this property is "kept, not a
coincidence" — say instead that lint enforces it.
