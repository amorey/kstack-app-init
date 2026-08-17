---
title: Per-cluster connection throughput
scope: sidecar
status: Planned
---

# Per-cluster connection throughput

## Goal

Live up/down throughput, per cluster, for the connections `kubeconn` holds.

The number has to mean bytes on the wire. A watch frame is mostly HPACK-compressed headers and
gzipped payload, so anything measured above the transport under-reports the busiest clusters by the
most.

## Depends on: one connection per cluster

Attribution is exact only because the socket belongs to one cluster. That is
→ [spec: one connection per cluster](per-cluster-connections.md), which lands first and stands on
its own. Without it a pool entry can carry several clusters' traffic, and no meter under it can
split them back apart.

## Where the bytes are counted

**`rest.Config.Dial` sits below TLS.** Its own doc says it creates "unencrypted TCP connections"
(`rest/config.go:152`); the transport layers TLS on top. A `net.Conn` wrapper there counts
ciphertext: TLS records, HTTP/2 framing, HPACK, gzip, the handshake, and the keepalive PINGs. That
is the definition of throughput we want, and it is the only meter this spec needs.

The plumbing is `rest.Config.Dial` → `transport.DialHolder` (`rest/transport.go:115`) → the
transport's `DialContext` (`transport/cache.go:117-152`). client-go's `certRotatingDialer` wraps
ours rather than replacing it, so ours stays underneath and sees every byte.

Measuring in a `RoundTripper` instead would be the wrong layer: it sees decoded, decompressed
bodies, no framing or TLS overhead, and no connection-level frames at all.

## Design

`build` sets `own.Dial` to the entry's metering dialer. Every `net.Conn` it returns holds a pointer
to that entry's counters and adds into them:

```go
// meter accumulates one pool entry's wire bytes, across every connection it has
// dialed. On the entry rather than the connection: a lost ping, a GOAWAY or the 90s
// idle timeout replaces the socket, and a per-connection counter would restart there.
type meter struct{ rx, tx atomic.Uint64 }

type meteredConn struct {
	net.Conn
	m *meter
}

func (c meteredConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.m.rx.Add(uint64(n))
	return n, err
}
```

One atomic add per syscall, not per byte — `Read` returns up to 16 KB at a time, so the add
disappears against the syscall.

```go
// Throughput is a connection's byte counts, monotonic for the life of the pool entry.
type Throughput struct{ Rx, Tx uint64 }

// Totals reports what these credentials have moved on the wire. ok=false for a key the
// pool does not hold, including one it has dropped.
func (s *Service) Totals(key string) (Throughput, bool)
```

Rates are the caller's: sample twice, subtract, divide by elapsed. `kubeconn` publishes totals and
never a rate — it has no opinion about the window a consumer wants to average over.

**Totals reset when the key changes.** A new key is a new entry whose counters start at zero, so a
consumer differencing two readings sees a decrease. That is an ordinary counter reset: treat a
decrease as one and contribute nothing for that interval. The prober builds the key, so it is the
one that knows — for both halves of it, since the key carries the cluster id as well as the
credential fingerprint. A record deleted and recreated comes back under a fresh `ClusterID`, which
resets the counters exactly as a credential rotation does.

## Rules

- **Throughput never touches a record.** It is a measurement, so it rides a gauge, the same shape as
  `clusterCacheHealthWatch`. A byte count written into `ClusterStatus` would push a `Modified` per
  cluster per sample to every webview, and behind each one a cache reconcile and a catalog pass.
  This is the trap `lastConnectedAt` is deliberately left null for.
- **Measure at the dialer**, which is the only layer that sees the wire.
- **Counters live on the pool entry.** Connections are replaced under a live entry.
- **The key stays opaque to `kubeconn`.** Sharing granularity is the caller's decision.
- **Never report a rate from `kubeconn`.** Totals only; the window belongs to the consumer.

## Build order

Each step is one red/green cycle and one commit.

1. The wire meter and `Service.Totals`, against an `httptest` server. The assertion worth making is
   that the counted bytes **exceed** the payload — that is what proves the meter sits below TLS and
   framing rather than above them. Also that a redial keeps accumulating, since that is what puts
   the counters on the entry.
2. `clustersvc` exposes the gauge, folding across a key rotation as a counter reset.
3. The schema's throughput gauge and the webview reading it.

Step 1 is `kubeconn` alone and lands before anything consumes it. Step 2 needs the prober and the
per-cluster keying above it.

## Not in this pass

- **A per-kind or per-request breakdown.** The socket is the finest grain this measures. Splitting a
  cluster's traffic across the kinds it syncs needs accounting above the transport, in different
  units, and is worth it only if someone asks which kind is expensive.
- **History.** `Totals` is a running count and the gauge is instantaneous. A sparkline needs a ring
  buffer somewhere, and the honest place is the webview, which already re-renders per sample.
- **Cache or disk bytes.** This measures the network only. What a sync worker writes to its on-disk
  cache is a different meter with a different home.
- **Anything for a connection nobody holds.** A parked cluster has no socket, so its throughput is
  zero — correctly, but a UI has to render that as "not connected" rather than "0 Mbps".

## Done when

Run the app against two clusters, sync one and leave the other idle, and the busy cluster's
throughput tracks what it is actually pulling while the idle one sits at the keepalive floor. Point
two kube-contexts at one cluster and each reports its own traffic.

Docs land in the same commits: `sidecar/CLAUDE.md`'s `internal/kubeconn` section gains the meter,
and the gauge rule in the `clustersvc` section gains throughput beside `lastConnectedAt`. Delete
this spec when the last step lands.
