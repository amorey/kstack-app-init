---
title: Diagnostics are redacted and bounded; a malformed credential field fails closed
scope: sidecar
status: Planned
---

# Diagnostics are redacted and bounded; a malformed credential field fails closed

**Needs:** nothing. **Hands on:** nothing. Closes R-06.

## Goal

Two gaps that both end with a credential somewhere it should not be.

**One — errors go to the log whole.** The GraphQL server is careful: its error presenter logs the
operation type and the Go error *type*, never the request text or the message
(`graph/server.go`, pinned by `TestErrorLogOmitsRequestData`). Nothing else in the sidecar is.
Roughly twenty `slog.Error`/`slog.Warn` calls pass `"err", err` straight through, and the ones that
matter are the ones nearest the credentials:

- `internal/auth/auth.go` logs sign-in failures and persist failures. An OAuth error commonly
  carries the token endpoint's response body and the URL it was fetched from.
- `internal/auth/grant.go` logs a credential-store read failure.
- `internal/clustersvc/**` logs sync, cache and janitor failures; a client-go error can carry a
  request URL, and a URL can carry a token.

The host forwards every sidecar line into its own tracing pipeline
(`src-tauri/src/services/sidecar/logs.rs`), so whatever is logged reaches the app's log sink too.

**Two — a credential field the redactor cannot parse is stored in the clear.** Writes redact by the
body's own group and kind (`kubestore/objects.go`), which is right, and `redact` deliberately
leaves a path alone when the value is not the shape the schema promises. That is fail-**open**: a
core `Secret` whose `data` is not a map skips `redactMapValues`, and the original bytes go into the
cache file.

## What is true now

- **Persisted messages are bounded but not redacted.** `clustersvc.TruncateMessage` caps a
  condition or event message at `MaxMessageLen` (200 bytes). Logs have no bound at all.
- **`logs.rs` forwards verbatim.** The host classifies a sidecar line by its own `level` and
  re-emits it, nesting the sidecar's fields under `sidecar.fields`. It does not, and should not,
  redact — which means every redaction has to happen sidecar-side, before the line is written.
- **The redaction table is correct about *which* object it is looking at.** `redactionsFor` reads
  the body's `apiVersion`/`kind`, not the worker's configured kind, so addressing an object as
  something else does not bypass it. That property has no test.
- **The table is deliberately incomplete** and says so; this spec does not try to complete it.

## Design

### A redaction helper, applied at the boundaries

One function, in a package both `auth` and `clustersvc` can reach:

```go
// Safe renders err for a log or a stored message: bounded, and with the shapes that
// carry credentials removed. What it strips is what a credential looks like in an error
// — a URL's query and userinfo, an Authorization or Set-Cookie header echoed back, a
// bearer/JWT-shaped token — not a keyword list, which is how the next format slips past.
func Safe(err error) string
```

It is a *renderer*, not a logging wrapper: call sites keep `slog.Error("…", "err", safe.Safe(err))`,
so the change is visible in the diff and a new call site is an ordinary review catch.

Bounding is part of it, because an unbounded error string in a log line is its own problem (a
`/readyz?verbose=true` body runs to kilobytes). **It is not `MaxMessageLen`.** That 200-byte cap
exists because a condition message is re-read on every frame of a whole-fleet watch; a log line is
read once, by a human, and 200 bytes routinely truncates a client-go or OAuth error before the part
that says what went wrong. Use a separate, larger cap — order of a kibibyte or two — and leave
`MaxMessageLen` to persisted messages.

**Where it goes:** every `slog` call in `internal/auth/**` and `internal/clustersvc/**` that passes
an `err`, plus `main.go`'s startup errors. Not the supervisor's panic logs, which carry our own
stack, not a server's.

### The redactor fails closed

`redact` gains one rule: **a path that exists but does not match its expected shape is dropped**,
not skipped.

The discrimination is the returned error, not the `found` boolean. `NestedString` and `NestedMap`
each return `(value, found, err)`, and a field present at the path but of the wrong type comes back
as `found == false` **with a non-nil err** — which today's code discards into `_`. So "absent" and
"there but unreadable" are currently the same branch, and both skip.

```go
// A value we cannot read is a value we cannot prove is safe. Dropping it loses a
// diagnostic; keeping it can store a credential. An err from a Nested* read means the
// path is occupied by the wrong type — not that it is missing, which stays a no-op.
```

`redactValue` on a non-string and `redactMapValues` on a non-map both become `RemoveNestedField`. A
path that is *absent* still costs nothing and stays a no-op — that is the common case, not a
failure.

## Rules

- **A logged error is rendered, never passed.** `"err", err` is the thing to grep for.
- **Redaction reads the body's own group and kind.** Never the caller's idea of what it fetched.
- **An unreadable credential field is dropped.** Never stored because it did not parse.

## Build order

Each step is one commit.

1. `Safe`, with table tests over the shapes: a URL with a query and userinfo, an echoed
   `Authorization` header, a bearer-looking token in free text, and an over-long message. Assert
   the *diagnostic* survives — a redactor that returns "error" is useless and would be adopted
   grudgingly.
2. Apply it across `auth` and `clustersvc`, plus `main.go`. One sentinel test per subsystem: seed a
   failure whose error carries a marker string, assert the marker is absent from the emitted record
   through a `slog` handler the test installs.
3. Fail-closed `redact`, with tests for a non-map `data` on a core `Secret`, a non-string password
   path, and a body whose `apiVersion`/`kind` disagree with the kind it was synced as. Assert on
   the err branch specifically, so a later refactor that collapses it back into `!ok` fails.
4. A Go-side sentinel at the writer: the marker is absent from the bytes the sidecar's `slog`
   handler emits. That is the boundary that matters, because the host forwards whatever it is
   given — a Rust test in `logs.rs` would only prove the forwarder forwards.

## Not in this pass

- **Completing the redaction table.** It is explicitly a long tail, and the cache's shape does not
  depend on it. Add entries when real operators turn up.
- **Structured redaction of cluster object bodies in transit.** The mirror already holds them; the
  boundary that matters is the file and the log, not the socket.
- **Terminal control sequences.** They arrive with the log-tail and exec windows, which do not
  exist. `X-2` tracks it.
- **A lint rule banning `"err", err`.** Worth wanting, but Go has no cheap seam for it here, and a
  rule that mostly fires on false positives gets suppressed.

## When it lands

- `security-model.md`'s GraphQL-error-log row loses its "Other subsystems' diagnostic errors are
  not covered (R-06)" caveat, and a new row lands: *Diagnostic errors rendered redacted and bounded
  before logging or persistence*, **Enforced** by the sentinel tests. A second row records the
  fail-closed redactor.
- The **R-06** bullet leaves `TODO.md`.
- Delete this spec.

## Done when

Point the app at an unreachable cloud endpoint and a cluster that rejects its credentials, and the
whole log — sidecar and host — carries no token, no query string and no line over the cap. A
handwritten `Secret` with a malformed `data` lands in the cache with the field gone.
