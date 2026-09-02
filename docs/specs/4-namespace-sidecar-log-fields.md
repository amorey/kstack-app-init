---
title: Sidecar log fields ride under a sidecar.* name
scope: host
status: Planned
---

# Sidecar log fields ride under a `sidecar.*` name

**Needs:** nothing. **Hands on:** nothing.

## Goal

Make it impossible to mistake a field the sidecar reported for a field the host recorded.

`forward_sidecar_line` (`src-tauri/src/services/sidecar/logs.rs`) re-emits each sidecar log line as
a host `tracing` event. Everything the sidecar's slog line carried beyond `level`/`msg`/`time` is
collected into `extra` and attached to the event as one field named `fields`, holding a JSON
string. Cluster-controlled strings reach that value — an object name, an event message — so a log
reader downstream sees a plain `fields` key with no hint that its contents came from outside the
host.

This is a naming nail-down, not a hole: the code already confines the sidecar's fields to one JSON
string rather than spreading them into separate tracing fields, so a sidecar line cannot shadow a
host field today. The rename is what keeps that true when someone reads the output.

## What to build

In `logs.rs`, rename the attached field from `fields` to `sidecar.fields`. The `emit!` macro needs
the quoted-name form, because a dotted name is not a Rust identifier:

```rust
tracing::$level!(target: "sidecar", "sidecar.fields" = %extra, "{}", msg);
```

Leave `target: "sidecar"` alone — it already marks the source, and `KSTACK_LOG_LEVEL` filters on it.

Then update the doc comment at the top of the file to say the sidecar's own fields never become
top-level host fields: they arrive as one JSON value under `sidecar.fields`.

## Tests

The existing tests in `logs.rs` cover `classify`, which does not change. Add one test that the
emitted event carries `sidecar.fields` and no bare `fields`. `tracing-subscriber` is already a
dependency with its default `fmt` feature: build a `fmt` subscriber writing into a shared
`Vec<u8>`, install it for the scope of the test with `tracing::subscriber::with_default`, forward
one line with extra fields, and assert the captured output contains `sidecar.fields=` and not
`fields=` at a word boundary.

If wiring a capturing subscriber turns out to cost more than the change is worth, skip the test and
say so in the security-model row — the change stays worth making.

## When it lands

In [`docs/security-model.md`](../security-model.md), move the row *"Sidecar-reported log fields
cannot shadow a host field"* out of **Not built**: **Enforced** if the test landed, **Held by
review** if it did not. `src-tauri/CLAUDE.md` does not describe log forwarding today; add one line
naming the field where the sidecar lifecycle is described.
