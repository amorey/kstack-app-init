---
title: Probe keys, no public ID
scope: sidecar
status: Planned
---

# Probe keys, no public ID

## Goal

Make the registration name the only public identity of a probe, and offer `Key[T]` — a typed
wrapper over that name — as an opt-in upgrade for read sites:

```go
// today
p.connection = probe.Register(e, nameConnection, &connectionProbe{...}, ...)   // ID kept
probe.WithDependencies(p.connection)
e.WakeAll(p.connection)
info := probe.Get[connInfo](snap, nameConnection)

// after
probe.Register(e, nameConnection, &connectionProbe{...}, probe.WithKey(keyConnection), ...)
probe.WithDependencies(nameConnection)
e.WakeAll(nameConnection)
info := keyConnection.From(snap)
```

`ID` becomes engine-internal. `Register` returns nothing.

## Why

`ID` has exactly three public jobs — the two edge options, and `Wake` — and every one can take
the name instead, which callers already hold as constants. The only reason `kubeconn`'s
`probeIDs` struct exists is to carry Register's returns to those call sites; with names, it is
deleted and registration stops threading handles nobody wants.

`Get[T](snap, name)` states the name↔type pairing at every read site, and states it unchecked: a
mistyped read panics only when a value lands. A `Key[T]` states the pairing once, beside the
probe, and `WithKey` moves the mismatch to a boot panic — where every other wiring bug in this
engine already fails.

## Design

**The name is the identity.** Edges and wakes resolve it at registration or call time against
`byName`; an unknown name panics as a wiring bug, exactly as an out-of-range ID does today. The
acyclicity guarantee is unchanged: `WithDependencies` and `WithWatches` still resolve at
registration time against probes already registered, so a forward reference panics at boot and
registration order stays topological.

**A key is a freestanding declaration, not a registration artifact.** `NewKey[T](name)` pairs a
name with a value type; `key.From(snap)` is `Get[T](snap, name)` with the pairing filled in. A
caller that declares no keys loses nothing — `Get` stays, and edges take plain names.

**`WithKey` is the eager check, and only that.** Passing it to `Register` panics at boot when the
key's name is not the registration name or its type is not the probe's `T`. It wires nothing:
a key works against a snapshot with or without it. The check is boot-time, not compile-time —
`ProbeOption` is type-erased, so the compiler cannot unify the key's `T` with Register's; what
`WithKey` buys is failing at startup instead of on the first committed value.

**The untyped walk goes by name too.** `Snapshot.Attempts` takes a name, and `Snapshot.Len` is
deleted — a reader walking every probe walks its own name list, which it already owns as the
constants it registered with.

## API

```go
// Key pairs a probe's registration name with its value type, declared once beside the probe.
type Key[T any] struct{ /* name string */ }

func NewKey[T any](name string) Key[T]
func (k Key[T]) Name() string
func (k Key[T]) From(snap Snapshot) Observation[T]  // Get[T](snap, k.Name())

// WithKey asserts at registration that k names this probe and matches its value type — a boot
// panic in place of Get's first-committed-value panic. Optional; it wires nothing.
func WithKey[T any](k Key[T]) ProbeOption

// Changed signatures — names in place of IDs. Unknown names panic, and the edges still resolve
// against probes already registered.
func Register[T any](e *Engine, name string, p Probe[T], opts ...ProbeOption)  // no return
func WithDependencies(names ...string) ProbeOption
func WithWatches(names ...string) ProbeOption
func (e *Engine) Wake(subjectName string, names ...string)
func (e *Engine) WakeAll(names ...string)
func (snap Snapshot) Attempts(name string) Attempts  // replaces Attempts(ID) and Len
```

## Engine changes

`ID` unexports to the slice index it already is; `byName` resolves every public name once, at the
boundary. `WithKey` needs a type token to check against: `Register[T]` holds `T`, and the option
stores a check closure (`reflect.TypeFor[T]` on each side) that `register` runs with the spec's
name — reflection at boot, never on the run path. Panic messages carry names instead of indices,
which they should anyway.

## How kubeconn maps

`probeIDs` and `registerProbes`' return are deleted; the edge options and `WakeAll` take
`nameConnection`. The one cross-probe read gets a key:

```go
var keyConnection = probe.NewKey[connInfo](nameConnection)

probe.Register(e, nameConnection, &connectionProbe{kubecfg: kubecfg},
    probe.WithKey(keyConnection), probe.WithInterval(30*time.Second))
```

`stateOf`'s reads become `keyConnection.From(v)`; `newsOf` walks a `probeNames` list (the five
constants) instead of `ID(0)..ID(numProbes-1)`, which retires `numProbes` as an array size in
favor of the list's length. The other four probes keep plain names until something reads them.

## What this deletes

`probe`: public `ID`, `Snapshot.Len`, Register's return. `kubeconn`: `probeIDs`, `numProbes` as
a standalone constant.

## Build order

1. `Key`, `NewKey`, `From`, `WithKey`; names through `Register`, the edges, `Wake`/`WakeAll`,
   `Snapshot.Attempts`; unexport `ID`.
2. Port the engine tests; add one each for `WithKey`'s two boot panics (wrong name, wrong type)
   and for an edge naming an unregistered probe.
3. Move `kubeconn`: delete `probeIDs`, key the connection read, walk names in `newsOf`.
4. Fold the landed contract into `sidecar/CLAUDE.md` and delete this spec.
