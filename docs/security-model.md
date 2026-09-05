# Security model

What the app protects, where the boundaries are, and which protections are enforced by a test
rather than by remembering. Present tense, like a `CLAUDE.md`: this file states what is true now.

Dated findings live in [`docs/security/`](security/) and are never rewritten. Gaps live in
[`TODO.md`](TODO.md#security), each carrying the shape of the work; one settled enough to build
straight through gets a spec in [`docs/specs/`](specs/). A risk we decide to accept becomes an ADR,
so it can be told apart from one nobody has noticed.

Latest review and complete disposition register: [4 September 2026](security/2026-09-04-security-review.md).
Open findings are recorded work, not accepted risks unless an ADR explicitly says so. Tests pin
specific properties; they do not prove the absence of vulnerabilities or enforce repository branch
protection. Platform checks and advisory-scan limitations are listed in the review.

## Assets

- **Kubeconfig credentials**, including `exec` plugin invocations that mint live tokens.
- **The cluster mirror** — one SQLite file per cache holding full object bodies for every synced
  kind.
- **The IPC socket** — a credential-free path to that mirror and to every mutation.
- **The OAuth token set** in the OS keyring (access, refresh and ID tokens). The client supports
  retained or rotated refresh tokens; production issuer rotation policy is outside this repository.
- **Cluster availability** — the app holds standing watches on every mirrored kind.

## Boundaries

Four boundaries, in the order an attacker meets them:

1. **Webview → host.** The host exposes Tauri commands; no filesystem or shell plugin permission
   is granted to the page. Production CSP restricts script loads and connection APIs to local
   origins; it is not a general network sandbox. Navigation is restricted separately, by
   `build_window`'s default-deny callbacks. The host forwards GraphQL verbatim.
   Script execution can read the mirror and invoke all current app/account/cache mutations. The
   current schema exposes no arbitrary Kubernetes write or pod-exec operation. Custom host commands
   also allow window creation, host preferences and app exit.
2. **Host → sidecar.** One Unix socket (named pipe on Windows) carries GraphQL over HTTP/1.1 and
   gRPC over h2c. File mode/DACL restricts the account; the sidecar additionally checks the host PID
   supplied by the host. Standalone `--host-pid=0` permits the same account. Authentication runs
   both ways: the host reads the peer's PID from the kernel on every dial and refuses anything that
   is not the child it spawned. On Unix the endpoint sits in an owner-only directory; the Windows
   pipe namespace is flat, so there the DACL and the peer check are the whole policy.
3. **Sidecar → clusters.** Credentials and TLS policy both come from the user's kubeconfig. Everything
   a watch returns is attacker-controlled if the cluster is.
4. **Sidecar → cloud.** OIDC sign-in through the system browser and a loopback redirect; prefs sync
   over HTTPS with a bearer token. The implemented settings payload contains `theme` and `locale`;
   OAuth also transmits protocol credentials and identifiers, and services see normal connection
   metadata. There is no implemented cluster-to-cloud or chat data path. Cloud sign-out does not
   revoke Kubernetes credentials, stop cluster access, or erase caches, and revoking its own
   grant is best-effort: the local credential is cleared first and the revoke is fired detached,
   so an unreachable issuer or a quit leaves the grant live at the issuer.

## Protections

**Enforced** means a named automated check detects the specified regression when run. CI defines
these checks, but required-check and release-gating settings are not established by this file. **Held by review** means it is true today and
nothing stops the next change undoing it. **By decision** means the gap is a risk we accepted, and
links the ADR that accepted it. **Not built** links the work.

| Protection | Where | Status |
| --- | --- | --- |
| IPC socket bound owner-only, with no world-accessible window between bind and chmod | `sidecar/internal/ipc/listen_unix.go` | **Enforced** — `TestListen_IsOwnerOnly` |
| Named pipe restricted to the owner by DACL | `sidecar/internal/ipc/listen_windows.go` | **Enforced** — `TestOwnerOnlyDACL_ParsesAndIsRestrictive` |
| The sidecar accepts only the host PID when launched by the host | `sidecar/internal/ipc/authlistener.go` | **Enforced** — `TestAuthenticated_RejectsForeignPeer` |
| Secret values redacted at write time, keyed off the body's own group and kind | `kubestore/objects.go` | **Enforced** — `TestProjectRedactsSecretValues`, `TestProjectRedactsOnlyCoreSecrets` |
| `managedFields` and the last-applied annotation stripped before storage | `kubestore/objects.go` | **Enforced** — `TestProjectStripsServerNoise` |
| Loopback callback checks `state` before consuming a code or an error | `sidecar/internal/auth/login.go` | **Enforced** — `TestLoopbackRejectsInvalidCallbackWithoutConsuming` |
| A login completing after sign-out cannot restore the session, and its token is revoked | `sidecar/internal/auth/grant.go`, `auth.go` | **Enforced** — `TestLogoutBeatsAPendingLogin`, `TestSupersededLoginRevokesItsToken`, `TestASecondLoginSupersedesTheFirst`; the epoch check shares `set`'s lock with the write it guards |
| The loopback callback bounds its reads, and a repeated error cannot park its handler | `sidecar/internal/auth/login.go` | **Enforced** — `TestLoopbackServerBoundsItsReads`, `TestLoopbackSurvivesRepeatedAuthorizationErrors` |
| A webview's subscriptions are torn down by the host on reload and close | `src-tauri/src/lib.rs` | **Enforced** — `cancel_webview_drops_only_that_webviews_subscriptions` |
| Sidecar-reported log fields ride under `sidecar.fields`, never as host fields | `src-tauri/src/services/sidecar/logs.rs` | **Enforced** — `forwarded_fields_are_namespaced` |
| `tauri.conf.json` declares no windows; chrome decisions stay in `build_window` | `src-tauri/src/window_manager.rs` | **Enforced** — unit test in `window_manager.rs` |
| Data directories created 0700; `host.json` and the sync files written 0600 | `src-tauri/src/services/sidecar/service.rs`, `atomicjson`, `clustersvc/service.go`, `sqlitemigrate`, `appdb`, `kubestore/manager.go` | **Enforced** on Unix — `ensure_data_dir_creates_and_tightens_to_0700`, `TestSaveIsOwnerOnly`, `TestOpenPoolFilesAreOwnerOnly`, `TestCacheFileIsOwnerOnly`, `TestSetOwnerOnlyUmask`; on Windows the inherited profile ACL, [by decision](adr/2026-09-02-windows-cache-files-rely-on-the-profile-acl.md) |
| Cloud and OAuth endpoints come from the host's arguments, not from inherited environment | `src-tauri/src/services/sidecar/service.rs`, `sidecar/config.go` | **Enforced** — `cmd_args_passes_socket_data_dir_host_pid_and_endpoints`, `TestConfigFromArgsIgnoresEnvironment`, plus the untagged-build assertion in `release.yml` |
| Go, Rust and npm advisory jobs configured on every pull request | `.github/workflows/ci.yml`, `src-tauri/.cargo/audit.toml` | **Enforced** — the `Go · Audit`, `Rust · Audit` and `TypeScript · Audit` jobs; npm fails at moderate, Rust denies unsound and unmaintained against a reasoned ignore list ([the advisories Tauri's dependency graph pins](adr/2026-09-05-tauri-pinned-transitive-advisories.md)). Advisories run per pull request, not on a schedule, and releases do not depend on these jobs (R-08) |
| Production CSP admits no remote script, no `eval`, no frames, no form posts | `src-tauri/tauri.conf.json` | **Enforced** — `production_csp_admits_no_remote_code` reads the shipped config: `script-src` is `'self'` alone, the four `'none'` directives hold, and no directive admits `unsafe-eval` or an origin off this machine |
| The webview renders no HTML from cluster data — no `innerHTML`, no `dangerouslySetInnerHTML` | `src/` | **Enforced** — the `custom/cluster-data-is-text` config object in `eslint.config.ts` |
| Printer columns evaluated by a restricted reader, not a template engine | `src/lib/jsonpath.ts` | **Enforced** — `src/lib/jsonpath.test.ts` pins the reader; the `custom/cluster-data-is-text` config object refuses a template engine, by import and by call |
| An unverified cluster connection is visible in the UI | `src/lib/kube-config.tsx` (verdict), `clustersvc/clusters.go` (the mirrored entry) | **Enforced** — `tlsUnverifiedReason`'s tests and the context bar's two badge tests; the sidecar draws no verdict, so the frontend tests are the fence |
| GraphQL error logs exclude request text, values, names, aliases and error messages | `sidecar/graph/server.go` | **Enforced** — `TestErrorLogOmitsRequestData`; only operation type and Go error type are logged |
| Every log record is rendered redacted and bounded at the sink, whoever wrote it | `sidecar/internal/logging/redact.go`, `sidecar/internal/safe/safe.go` | **Enforced** — `safe_test.go` pins the shapes stripped (a URL's query and userinfo, an echoed `Authorization`/`Set-Cookie`, a bearer/JWT token) and the cap; `TestInitRedactsWhatACallerDidNotRender` covers message, attr, `With` attr and group, so a dependency logging its error whole (beehive, client-go, oauth2) is redacted too; `TestStoreLoadErrorIsLoggedWithoutTheCredential` and `TestCachePassLogsAFailureWithoutTheCredential` are subsystem sentinels through the same sink. A value that is neither string nor error — a struct behind `slog.Any` — is encoded past the renderer |
| A diagnostic persisted on a record is rendered where it is recorded | `sidecar/internal/supervisor/result.go`, `kubesync/service.go` | **Enforced** — `TestFailRendersTheMessageItStores`, `TestArmRendersWhatADriverErrorCanSay`. A condition or discovery message outlives its log line and is served to the UI, so the sink above cannot cover it; `Result.Err` keeps the original for a caller matching on it |
| A credential field the redactor cannot parse is dropped, not stored | `kubestore/objects.go` | **Enforced** — `TestSanitizeDropsASecretWhoseDataIsNotAMap`, `TestSanitizeDropsAPasswordThatIsNotAString`, and `TestNestedReadsDistinguishAnAbsentPathFromAnUnreadableOne` pins the err branch the discrimination rests on. `TestSanitizeRedactsByTheBodysOwnGroupAndKind` pins that the table is chosen from the body, not from how the object was addressed |
| Login ID tokens verified against JWKS; restored identity is display-only | `sidecar/internal/auth/oauth/oauth.go`, `grant.go` | **Held by review** for authorization use: `UnverifiedIdentity` distinguishes unchecked claims, but `DisplayOnly` converts to `Identity`, so the compiler does not prevent later misuse. Startup `authenticated` means a stored refresh token exists, not that the server has just validated it |
| Auth tokens are absent from the GraphQL auth projection | `sidecar/graph/schema.graphqls` | **Enforced** — `TestAuthProjectionCarriesNoTokens` pins `AuthState`'s and `Identity`'s field sets, the two types gqlgen binds onto the Go values that hold the token set |
| Every non-watch Kubernetes request carries an idle-read bound | `kubeconn/idletimeout.go` | **Enforced** — `TestNewConnectionBoundsItsNonWatchReads` pins the wrapper onto every connection, and the behaviour is covered either side of it (`TestIdleTimeoutCancelsAStalledBody`, `TestIdleTimeoutCancelsAStallBeforeHeaders`, `TestIdleTimeoutLetsASlowButStreamingBodyFinish`, `TestIdleTimeoutExemptsAWatch`, `TestIdleTimeoutLeavesACallersCancelAlone`) |
| No permission granted to the webview ahead of a consumer | `src-tauri/capabilities/default.json` | **Enforced** — `default_capability_grants_only_window_chrome` pins the permission list |
| No GraphQL field served ahead of a consumer | `sidecar/graph/schema.graphqls` | **By decision** — [the schema's breadth is held by review](adr/2026-09-03-schema-breadth-is-held-by-review.md); a mutation with no caller is what review is for |
| Cache contents left unencrypted, protected by file mode/ACL; disk encryption is an operator responsibility | `kubestore/manager.go`, `sqlitemigrate` | **By decision** — [the cache is ordinary application data](adr/2026-09-02-the-cache-is-ordinary-application-data.md) |
| Cloud SSE frames have an aggregate payload limit of 1 MiB | `sidecar/internal/cloud/api/api.go` | **Enforced** — `TestParseSSEBoundsAggregateFrame`; the line limit alone is insufficient |
| A retention policy, so a cache stops outliving the user's interest in its cluster | — | **Not built** — no spec yet; [TODO](TODO.md#security) |
| A gesture before a kubeconfig `exec` plugin runs | — | **Not built** — no spec yet; [TODO](TODO.md#security). Every imported context is dialled, so a kubeconfig write runs the plugin it names |
| A size ceiling on a cache | `kubestore/janitor.go`, `clustersvc/caches.go` | **Built** — the janitor judges each sweep and publishes the edge (`TestSweepMarksAFileOverItsLimit`, `TestSweepPublishesOnlyWhenTheVerdictChanges`, `TestStatsReportsTheJanitorsVerdict`), and the cache pass stops the sync and says why (`TestCachePassStopsACacheOverItsSizeLimit`, `TestCachePassKeepsACacheStoppedOnceItsFileIsClosed`, `TestCachePassDoesNotStopACacheOnBytesAlone`, `TestCachesWatchHealthReportsACacheStoppedByItsCeiling`, `TestClearingACacheReleasesItsSizeStop`). Soft by one sweep interval, and a stopped cache stays stopped until the user clears it — [a stopped cache is held by its record](adr/2026-09-03-a-stopped-cache-is-held-by-its-record.md) |
| Retention on cached Kubernetes events, beyond the relist's prune | — | **By decision** — [bound the cache by total size](adr/2026-09-03-bound-the-cache-by-total-size.md); the ceiling above is what bounds them |
| The host forwards only operations the app ships | — | **By decision, not built** — [no GraphQL operation allowlist](adr/2026-09-03-no-graphql-operation-allowlist.md); CSP reduces injection risk but does not restrict the authority of permitted bundled code. What the schema's breadth costs is S-9's other half, [spec 6](specs/6-resource-budgets.md) |
| The app tells the user a newer release exists | — | **Not built** — [spec 3](specs/3-update-notification.md). Nothing checks, so a user can sit on a version with a known bug indefinitely |
| Signed in-app updates | — | **Not built** — [spec 9](specs/9-signed-in-app-updates.md). The release workflow configures signed macOS/Windows downloads and GPG-signed checksums; Linux bundles are unsigned. Actual signatures and signing policy require release-artifact verification |
| Sign-out clears the local credential, and nothing is revoked before it | `sidecar/internal/auth/auth.go` | **Enforced** — `TestLogoutRevokesAfterClearingAndIgnoresRevokeFailure` holds the clear open and fails on a revoke that arrives during it; `TestLogoutKeepsSessionWhenCredentialClearFails` pins the failed clear staying signed in. The server-side revoke beside it is **not a promise**: it is fire-and-forget, so a failed revoke, or a quit before it finishes, leaves the grant live while the app reports signed out |
| Top-level navigation restricted to the app origin | `src-tauri/src/window_manager.rs` | **Enforced** — `build_window` installs `on_navigation` and `on_new_window`, both default-deny, pinned by `build_window_installs_the_navigation_policy`; the allowlist is `is_app_origin` (`the_bundled_app_origins_are_admitted`, `everything_that_is_not_the_app_is_refused`, `the_dev_server_is_admitted_only_by_a_debug_build`). CSP does not govern navigation. Per-engine firing is verified by hand, not by a test |
| The host refuses any IPC peer that is not the sidecar it spawned | `src-tauri/src/services/sidecar/{ipc,peer}.rs` | **Enforced** — `connect_refuses_a_peer_that_is_not_the_expected_process` binds a hostile listener in the test process and asserts the dial is refused; `query_refuses_a_server_that_is_not_the_sidecar` pins that the check is wired into the transport, not merely available. `connect_refuses_when_no_sidecar_is_running` covers the exited child, whose PID the OS may have reassigned. The Windows arm compiles from the same code but is exercised only by a native run |
| Query cost, concurrency, fan-out and total-disk budgets | — | **Not built** — [spec 6](specs/6-resource-budgets.md). The outer request-size and timeout bounds above are not budgets on the work an operation causes |
| Third-party actions pinned to SHAs, and a release gated on the checks for its own commit | — | **Not built** — [spec 7](specs/7-release-assurance.md). Actions float on tags (`dtolnay/rust-toolchain@master` on a branch), `release.yml` grants write permissions to every job, and audits run only per pull request |
| The macOS bundle grants only the hardened-runtime exceptions it needs | `src-tauri/entitlements.plist` | **Not built** — [spec 8](specs/8-macos-entitlements.md). Three exceptions ship whose necessity has never been tested against a signed build |

## Two facts that shape everything

**The webview is fully trusted by the host.** `graphql_query` forwards the operation string
unexamined, so a script running in the page can read every mirrored object and call every mutation.
CSP and text rendering reduce injection risk; bundled malicious JavaScript is already permitted
by `script-src 'self'`. Navigation off the app origin is refused, so the script stays in the app's
own page — it does not stay out of the app's authority.

**The sidecar authenticates a process, not a principal.** With a nonzero host PID, the endpoint serves that process only — the kernel supplies the peer's pid, so it cannot be claimed. But an attacker who can
run code *inside* the host process, or debug it, inherits the whole surface, and the file mode still
carries the rest of the policy.

These controls do not isolate the app from a compromised user account or host process. The
current mutation surface is local app/cache/account state; future Kubernetes writes, terminal
execution, or assistant egress require a new boundary review.

## How this is recorded

- **This file** — what is true now. A row moves to *Enforced* when the mechanism holding it lands,
  never because the code looks right.
- **[`docs/security/`](security/)** — dated review records, append-only, each naming the commit it
  reviewed. Like ADRs, they are exempt from the describe-the-present rule: a review says what was
  believed on the day. A later review is a new file, not an edit.
- **[`TODO.md`](TODO.md#security)** — the gaps. They stay out of the table above except as *Not
  built* rows, so nothing here reads as a protection that exists.
- **`docs/adr/`** — a risk we decide to accept. An accepted risk is a decision with a cost, and
  writing it down is what stops the next review re-litigating it.
