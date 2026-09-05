---
title: The native webview refuses to navigate
scope: src-tauri
status: Planned
---

# The native webview refuses to navigate

**Needs:** nothing. **Hands on:** nothing. Closes R-02.

## Goal

Keep the app window on the app. Today nothing stops it going somewhere else.

`build_window` (`src-tauri/src/window_manager.rs`) sets title, size, chrome and the boot script,
and installs **no navigation or new-window policy**. Anything running in the page can set
`location.href` to a remote origin and the window follows it. Once it does, the page on screen is
someone else's, and it is inside a window that holds every host command.

The CSP is not the answer here, and `security-model.md` already says so. `connect-src` governs
fetch and WebSocket; it does not govern top-level navigation. A script can also put data in the
URL it navigates to, which is an egress path CSP never sees.

## What is true now

- Production CSP is tight — `script-src 'self'`, four `'none'` directives — and is pinned by
  `production_csp_admits_no_remote_code`.
- The webview loads `WebviewUrl::default()`; every route is client-side under TanStack Router.
- **`src/` contains no external link and no `window.open`.** The only outbound URL the app opens is
  host-owned: the tray's account-settings item, through `tauri_plugin_opener` (`src/tray/mod.rs`).
  So the policy below costs the current app nothing.

## Settle this first

**Does Tauri 2.11.5's `WebviewWindowBuilder` expose a new-window hook?** `on_navigation` it
certainly has. The new-window request handler is a wry API
(`with_new_window_req_handler`), and Tauri may not re-export it on the builder. Check before
committing to the sketch below. If it is absent, the answer is not to invent one: the app ships no
`window.open` and no `target="_blank"`, so the containment is a lint rule holding that true, plus
whatever each engine does with a popup from a page that cannot navigate. Say which it is in the
same edit.

## Design

**Two callbacks on the builder, both default-deny** (the second subject to the check above).

```rust
// The app is a local single-page bundle: every legitimate navigation is
// client-side routing under the app origin. Anything else is a page trying to
// leave, and a window that has left still holds every host command.
builder = builder
    .on_navigation(|url| is_app_origin(url))
    .on_new_window(|_url| false); // nothing in-app opens a window this way
```

`is_app_origin` is the whole decision, and it is a pure function with its own tests — that is what
makes this testable without a webview. It admits exactly what the app loads:

- `tauri://localhost` and `http://tauri.localhost` — the bundled app origin, per platform.
- `http://localhost:<port>` **only** under `#[cfg(debug_assertions)]`, for `pnpm tauri dev`.
- `about:blank`, which some engines hand the callback during startup.

Client-side routing is unaffected: TanStack Router navigates with `pushState`, which is not a
document navigation and never reaches the handler.

Everything else is refused: remote origins, `file:`, `data:`, `javascript:`, and any scheme we did
not list. **Default-deny by shape** — the function matches an allowlist and returns false at the
end, so a scheme nobody thought of is refused rather than admitted.

**An external link stays host-owned.** When the app wants one — a docs link, the cloud dashboard —
it goes through the opener plugin from the host, as the tray already does, and never through
navigation. That is a rule, not a mechanism; the mechanism is that navigation is refused.

## Rules

- **The webview never navigates off the app origin.** If a feature seems to need it, it needs a
  host command instead.
- **`is_app_origin` is an allowlist that ends in `false`.** Never a denylist of bad schemes.
- **Debug origins are behind `cfg`,** so a release build cannot admit `localhost`.

## Build order

One commit, but three assertions.

1. `is_app_origin` unit tests: the app origins pass; a remote origin, `file:`, `data:`,
   `javascript:` and an unknown scheme all fail; the dev origin fails in a non-debug build.
2. A `window_manager` test that `build_window` installs both callbacks — the same shape as the
   existing chrome test, so the policy cannot be dropped by an edit to the builder.
3. `security-model.md` stops saying no navigation restriction is configured — it is the only file
   that says so, in its *Two facts that shape everything* section.

## Not in this pass

- **Per-window policy for the log-tail and exec windows.** They do not exist yet. When they land
  they go through `build_window` and inherit this.
- **Auditing what a permitted page may do.** A compromised bundled script still holds the whole
  GraphQL surface — that is H-1, accepted, and this changes nothing about it.
- **Verifying engine behaviour on all three platforms.** The callbacks are Tauri's, and their exact
  firing differs across WKWebView, WebView2 and WebKitGTK. Confirming that is the manual step under
  *Done when*, and it stays in the review's verification debt until someone runs it.

## When it lands

- `security-model.md`: the *Top-level navigation restricted to the app origin* row flips from
  **Not built** to **Enforced** by the two tests, noting that per-engine behaviour is verified
  manually, and the *Two facts* section drops "No native navigation restriction is configured".
- The **R-02** bullet leaves `TODO.md`.
- Delete this spec.

## Done when

On each of macOS, Windows and Linux, a devtools console in a release-ish build runs
`location.href = 'https://example.com'` and the window does not move. `pnpm tauri dev` still loads.
The tray's account link still opens in the system browser.
