---
title: The host passes the sidecar's endpoints as arguments
scope: host · sidecar · build · ci
status: Planned
---

# The host passes the sidecar's endpoints as arguments

**Needs:** nothing. **Hands on:** nothing.

## Goal

Stop letting the launch environment decide where the app signs in.

`sidecar/main.go` reads five values out of the environment:

```go
CloudURL:        envOr("KSTACK_CLOUD_API_URL", "https://api.kstack.sh"),
OAuthIssuerURL:  envOr("KSTACK_OAUTH_ISSUER", "https://oauth.kstack.sh"),
OAuthClientID:   envOr("KSTACK_OAUTH_CLIENT_ID", "kstack-desktop"),
KeychainService: os.Getenv("KSTACK_KEYCHAIN_SERVICE"),
DataDir:         flag "--data-dir", defaulting to KSTACK_DATA_DIR
```

The sidecar inherits the host's environment, and the host inherits whatever launched it. Setting a
variable in that environment is a lower bar than running code as the user: a `.envrc` in a cloned
repository, a devcontainer or IDE workspace setting, a `.desktop` file's `Env=`, a shell profile.
Any of them can point sign-in at an issuer the attacker controls and collect the resulting tokens,
and the user sees a normal browser sign-in.

The host already sets `KSTACK_KEYCHAIN_SERVICE` only in debug builds — but the sidecar reads it in
every build, so a release sidecar can still be pointed at a keyring entry of someone else's naming.
Same fix, same spec.

## What to build

### Sidecar

Turn the four values into flags, and make the environment a debug-only override:

```go
cloudURL := flag.String("cloud-url", "https://api.kstack.sh", "kstack cloud API base URL")
oauthIssuer := flag.String("oauth-issuer", "https://oauth.kstack.sh", "OAuth issuer URL")
oauthClientID := flag.String("oauth-client-id", "kstack-desktop", "OAuth client id")
keychainService := flag.String("keychain-service", "", "keyring service name; empty is the Kstack default")
```

Keep the env override behind a build tag, so a release binary cannot be redirected at all. Two
files beside `main.go`:

- `envoverride_debug.go` (`//go:build debug`) — applies `KSTACK_CLOUD_API_URL`,
  `KSTACK_OAUTH_ISSUER`, `KSTACK_OAUTH_CLIENT_ID` and `KSTACK_DATA_DIR` over the flag values, for
  a standalone dev run with no host to pass them.
- `envoverride.go` (`//go:build !debug`) — an empty function.

Then delete `envOr` and every `os.Getenv` but one from `main.go`. `KSTACK_LOG_LEVEL` stays as it
is: a log level redirects nothing.

**Make it testable.** Move the flag set and the override into a function `configFromArgs(args
[]string) (app.Config, error)` that `main` calls, so the tests below can run it in-process; the
build-tagged override is a function that function calls.

`--data-dir` is already a flag the host always passes; it only needs its env default moved into the
debug override. Two docs name `KSTACK_DATA_DIR` as an equal alternative to the flag and stop being
true: `sidecar/CLAUDE.md:5` and the error text in `internal/app/app.go:72`.

### Build

**There is no dev target to change.** `beforeDevCommand` and `beforeBuildCommand` in
`tauri.conf.json` both run `make sidecar`, which runs `scripts/build-sidecar.sh` with no notion of
which one called it. So:

1. Give the script a switch — `KSTACK_SIDECAR_TAGS`, defaulting to empty, appended to the `go build`
   line as `-tags "$KSTACK_SIDECAR_TAGS"`.
2. Split the two hooks: `beforeDevCommand` becomes `make sidecar-dev && pnpm dev`, with a
   `sidecar-dev` target in the `Makefile` that sets `KSTACK_SIDECAR_TAGS=debug`.
   `beforeBuildCommand` stays as it is, and so builds without the tag. `release.yml` calls the
   script directly and sets no tags, so the shipped sidecar is untagged by construction.
3. **Log the endpoints at startup.** `slog.Info("sidecar starting", …)` (`main.go:55`) names
   socket, pid, host_pid and data_dir; add `cloud_url` and `oauth_issuer`, so a support log says
   where a build signed in. Also drop the "(env-overridable)" comment above the `app.New` call,
   which stops being true along with `envOr`.
4. **Assert it in CI.** The failure mode is a release binary that is still redirectable, and it is
   silent. Go records its build settings in the binary, so the check is one line in `release.yml`'s
   `build-sidecar` job, right after the build step:

   ```sh
   go version -m src-tauri/binaries/kstack-sidecar-* | grep -q -- '-tags=' && { echo "sidecar built with tags"; exit 1; } || true
   ```

   No need to run the binary.

### Host

In `cmd_args` (`src-tauri/src/services/sidecar/service.rs:320`), append the three endpoint
arguments from constants defined in that file, and `--keychain-service Kstack-dev` under the
`cfg!(debug_assertions)` branch that today sets the env var — then delete the `.env(...)` call. Do
not read any of them from the host's own environment — that would move the problem, not fix it. A
dev run overrides them with the sidecar's debug build, not with the host.

## What this does not cover

The environment still shapes two things, on purpose. `KUBECONFIG` and `PATH` decide which
kubeconfig loads and which `exec` plugin binary runs — that is `kubectl` parity and
[spec 10](10-approve-exec-credential-plugins.md) is what gates it. And Go's TLS and HTTP clients
honour `HTTPS_PROXY`, and on Linux `SSL_CERT_FILE` / `SSL_CERT_DIR`: an attacker who can set the
environment *and* place a CA file can still interpose on the cloud connection. Pinning the cloud's
certificate would close that at the cost of every corporate proxy, so it stays open and is stated
here rather than left implied.

## Tests

- **Sidecar:** `configFromArgs(nil)` yields the production endpoints, **and** yields them
  unchanged with `KSTACK_OAUTH_ISSUER` set (`t.Setenv`). `go test ./...` builds without
  `-tags debug`, so the ordinary test binary is the untagged one and this pins the tag boundary
  directly — only `make sidecar-dev` produces the tagged build.
- The CI assertion is still needed: it tests something else, that the release path really did build
  untagged.
- **Host:** extend `cmd_args_passes_socket_data_dir_and_host_pid` to expect the new arguments.
  Rename it to match.

## When it lands

Move the row *"Cloud and OAuth endpoints come from the host's arguments, not from inherited
environment"* in [`docs/security-model.md`](../security-model.md) out of **Not built** to
**Enforced**. Update the `## Security invariants` section of `src-tauri/CLAUDE.md`, which currently
says the sidecar reads the four variables from the host's environment — that becomes the debug
build tag, named.
