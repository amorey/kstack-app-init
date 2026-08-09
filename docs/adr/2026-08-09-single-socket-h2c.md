---
title: Multiplex GraphQL and gRPC over one socket with h2c
date: 2026-08-09
scope: cross-cutting
status: Accepted
---

# Multiplex GraphQL and gRPC over one socket with h2c

## Context

The host and sidecar need two kinds of channel: the webview's GraphQL traffic (queries,
mutations, SSE subscriptions — HTTP/1.1) forwarded by the host, and a host-internal control
channel (the tray's auth-state watch, login/logout, the resync poke) for which gRPC is the
natural fit — but gRPC requires HTTP/2. The transport is a per-instance Unix domain socket
(named pipe on Windows), user-restricted, with no TLS to negotiate protocols over. This
decision predates this file; the constraint it satisfies is recoverable from the code.

## Decision

One socket carries both. The sidecar's composition root (`internal/app`) wraps its handler in
`h2c.NewHandler` with a dispatcher keyed on `grpcserver.IsGRPCRequest`: HTTP/2
`application/grpc` requests go to the gRPC server, everything else to the GraphQL mux. The
predicate lives in `sidecar/grpc/server.go` because it *is* the definition of a gRPC request;
the composition root owns the topology — the decision that the two surfaces share a socket.
tonic on the host side dials the same socket with HTTP/2 prior knowledge.

The split of surfaces is deliberate: the cluster surface is **GraphQL-only** (there is no gRPC
kube/cluster channel), because its consumer is the webview; gRPC carries only what the host
itself consumes.

Shutdown is shaped by this multiplex. grpc-go's `GracefulStop` panics on the
`ServeHTTP`/h2c path, so streams end on their own serving context (`NotifyShutdown`) and
`Stop` runs only after `DrainWithContext` has waited on both sub-servers' WaitGroups — the
essential wait for hijacked h2c gRPC streams that `http.Server.Shutdown` does not track. The
GraphQL SSE drain is per-request, deliberately **not** `http.Server.BaseContext`: a
`BaseContext` cancel would tear down the shared h2c connection carrying gRPC mid-stream.

## Alternatives considered

**Two sockets, one per protocol.** Rejected: doubles the lifecycle surface (two binds, two
readiness signals, two things to clean up per instance) for no isolation benefit — both ends
are the same two processes on the same machine.

**gRPC for everything, including the webview surface.** Rejected: the webview consumes
GraphQL through urql, and gqlgen's schema is also the frontend codegen source of truth.
Rebuilding that on gRPC-web adds a proxy layer for zero gain on a local socket.

**GraphQL for everything, including the host control channel.** Workable, but the host is
Rust: a typed proto contract (`proto/auth.proto`, `proto/poke.proto`) compiled by
`tonic-prost-build` gives the tray a generated client, where GraphQL would mean hand-written
Rust types mirroring the schema.

**A separate multiplexer package.** Rejected: the dispatch is three lines; a package would
own nothing but them, while splitting "what is a gRPC request" from its owner.

## Consequences

One socket to bind, ready-signal, and tear down. The cost is the shutdown subtlety above:
anyone touching sidecar shutdown must preserve the NotifyShutdown → Shutdown → Drain → Stop
order and must not introduce `BaseContext` cancellation. Because nothing touches
`BaseContext`, the two transports drain independently with no required ordering between them.

## Revisit when

A web (non-Tauri) deployment needs the GraphQL surface on TCP with TLS — ALPN then does the
protocol split natively and h2c stops being load-bearing.
