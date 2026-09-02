# Security model

What the app protects, where the boundaries are, and which protections are enforced by a test
rather than by remembering. Present tense, like a `CLAUDE.md`: this file states what is true now.

Dated findings live in [`docs/security/`](security/) and are never rewritten. Gaps live in
[`TODO.md`](TODO.md#security); a risk we decide to accept becomes an ADR, so it can be told apart
from one nobody has noticed.

## Assets

- **Kubeconfig credentials**, including `exec` plugin invocations that mint live tokens.
- **The cluster mirror** — one SQLite file per cache holding full object bodies for every synced
  kind.
- **The IPC socket** — a credential-free path to that mirror and to every mutation.
- **The OAuth refresh token** in the OS keyring. Static; Hydra does not rotate it.
- **Cluster availability** — the app holds standing watches on every mirrored kind.

## Boundaries

Four boundaries, in the order an attacker meets them:

1. **Webview → host.** Tauri commands only; the page has no network access and no filesystem or
   shell permission. The host forwards GraphQL operations verbatim, so script execution in the page
   carries the full cluster surface.
2. **Host → sidecar.** One Unix socket (named pipe on Windows) carrying GraphQL over HTTP/1.1 and
   gRPC over h2c. The file mode is the entire access-control policy: any process running as the user
   can connect.
3. **Sidecar → clusters.** Credentials and TLS policy both come from the user's kubeconfig. Everything
   a watch returns is attacker-controlled if the cluster is.
4. **Sidecar → cloud.** OIDC sign-in through the system browser and a loopback redirect; prefs sync
   over HTTPS with a bearer token. Only `theme` and `locale` leave the machine.

## Protections

**Enforced** means a test fails if it regresses. **Held by review** means it is true today and
nothing stops the next change undoing it. **Not built** links the work.

| Protection | Where | Status |
| --- | --- | --- |
| IPC socket bound owner-only, with no world-accessible window between bind and chmod | `sidecar/internal/ipc/listen_unix.go` | **Enforced** — `TestListen_IsOwnerOnly` |
| Named pipe restricted to the owner by DACL | `sidecar/internal/ipc/listen_windows.go` | **Enforced** — `TestOwnerOnlyDACL_ParsesAndIsRestrictive` |
| Only the host process may connect to the IPC endpoint | `sidecar/internal/ipc/authlistener.go` | **Enforced** — `TestAuthenticated_RejectsForeignPeer` |
| Secret values redacted at write time, keyed off the body's own group and kind | `kubestore/objects.go` | **Enforced** — `TestProjectRedactsSecretValues`, `TestProjectRedactsOnlyCoreSecrets` |
| `managedFields` and the last-applied annotation stripped before storage | `kubestore/objects.go` | **Enforced** — `TestProjectStripsServerNoise` |
| Loopback callback checks `state` before consuming a code or an error | `sidecar/internal/auth/login.go` | **Enforced** — `TestLoopbackRejectsInvalidCallbackWithoutConsuming` |
| A webview's subscriptions are torn down by the host on reload and close | `src-tauri/src/lib.rs` | **Enforced** — `cancel_webview_drops_only_that_webviews_subscriptions` |
| `tauri.conf.json` declares no windows; chrome decisions stay in `build_window` | `src-tauri/src/window_manager.rs` | **Enforced** — unit test in `window_manager.rs` |
| Data directories created 0700; `host.json` and the sync files written 0600 | `atomicjson`, `clustersvc/service.go`, `appdb`, `kubestore/manager.go` | **Held by review** — the SQLite files themselves take the umask |
| Production CSP admits no remote script, no `eval`, no frames, no form posts | `src-tauri/tauri.conf.json` | **Held by review** |
| The webview renders no HTML from cluster data — no `innerHTML`, no `dangerouslySetInnerHTML` | `src/` | **Held by review** — [TODO](TODO.md#security) proposes a lint rule |
| Printer columns evaluated by a restricted reader, not a template engine | `src/lib/jsonpath.ts` | **Held by review** |
| GraphQL errors log the operation but never `variables` | `sidecar/graph/server.go` | **Held by review** |
| ID tokens verified against the JWKS; the unverified decode is display-only | `sidecar/internal/auth/oauth/oauth.go` | **Held by review** — a comment is the only fence |
| Tokens never appear on the GraphQL surface | `sidecar/graph/schema.graphqls` | **Held by review** |
| Every non-watch Kubernetes request carries an idle-read bound | `kubeconn/idletimeout.go` | **Held by review** |
| Cache encryption at rest, and a retention policy for what it holds | — | **Not built** — [TODO](TODO.md#security) |
| A gesture before a kubeconfig `exec` plugin runs | — | **Not built** — [TODO](TODO.md#security) |
| Retention on cached Kubernetes events | — | **Not built** — [TODO](TODO.md#security) |
| Dependency advisory scanning in CI | — | **Not built** — [TODO](TODO.md#security) |
| Signed in-app updates | — | **Not built** — [TODO](TODO.md#security) |

## Two facts that shape everything

**The webview is fully trusted by the host.** `graphql_query` forwards the operation string
unexamined, so a script running in the page can read every mirrored object and call every mutation.
The CSP is the containment, and it is one dependency deep.

**The sidecar authenticates a process, not a principal.** The endpoint serves the host process and
nothing else — the kernel supplies the peer's pid, so it cannot be claimed. But an attacker who can
run code *inside* the host process, or debug it, inherits the whole surface, and the file mode still
carries the rest of the policy.

Both are defensible for a single-user desktop app. Both mean one containment failure is total, which
is why the rows above are worth keeping honest.

## How this is recorded

- **This file** — what is true now. A row moves to *Enforced* when a test lands, never because the
  code looks right.
- **[`docs/security/`](security/)** — dated review records, append-only, each naming the commit it
  reviewed. Like ADRs, they are exempt from the describe-the-present rule: a review says what was
  believed on the day. A later review is a new file, not an edit.
- **[`TODO.md`](TODO.md#security)** — the gaps. They stay out of the table above except as *Not
  built* rows, so nothing here reads as a protection that exists.
- **`docs/adr/`** — a risk we decide to accept. An accepted risk is a decision with a cost, and
  writing it down is what stops the next review re-litigating it.
