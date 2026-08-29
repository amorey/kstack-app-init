---
title: Prepare each statement once, not once per call
scope: sidecar
status: Planned
order: 6
---

# Prepare each statement once, not once per call

**Needs:** [specs 4](4-batched-uid-statements.md)–[5](5-deletes-return-their-uids.md) for
statement text that does not depend on its arguments. The open contract it compiles against —
a reader pool that refuses writes, and a pool that keeps the connections it opens — has landed.
**Hands on:** nothing. Last of the sequence because it is the largest, and because everything
before it exists to make it possible.

## Goal

Stop re-parsing every statement on every call.

`kubestore` issues literal SQL through `ExecContext`/`QueryContext` at ~39 sites. modernc has no
compiled-statement cache: `conn.exec` prepares, runs, and finalizes in a defer (`conn.go:969`), so
each call pays a full `sqlite3_prepare_v2` — parse, name resolution, and query plan — for text
that never changes.

`insertObjectRow` runs four to six statements per object, so **a 500-object relist page compiles
about 2,500 statements**, and every watch re-read recompiles its `SELECT` on every debounced burst.

What a prepared statement buys is **once per connection, not once per pool**: `db.PrepareContext`
does not compile on every pooled connection up front — database/sql prepares lazily per connection
and caches per `*sql.Stmt` — so a four-connection reader pool compiles each read up to four times
over its life. And a connection reaped by the pool's 5-minute idle timeout
finalizes what was compiled on it, so the next burst prepares again. Both are bounded by
connections and by quiet periods; the per-row and per-burst recompilation is what goes.

## Design

Beehive's shape, which this is a straight port of.

**A statement is named, not written at the call site.** A `stmtID` enum indexes a
`[numStmts]string` table of statement text.

**Each id also declares whether it writes** — beehive's `stmtWrites [numStmts]bool`, a second
table beside the text. It decides which set the reader prepares and which set a call routes to.

**Nothing but a test can check that declaration**, and the spec should not pretend otherwise. The
database cannot: preparing a write against `query_only` succeeds and fails only on execution. Nor
can `openFile` catch it by finding a nil slot — an id wrongly declared a read is prepared on the
reader successfully, and the same bool then routes calls there, so the nil slot is unreachable and
the first symptom is "attempt to write a readonly database" at runtime. The declaration is
hand-maintained across ~39 ids and drives both halves of its own enforcement, so step 3 pins it
from outside: a test that cross-checks each id's **text** against its bool, with an exemption list
that carries a reason per entry the way beehive's does.

**Both pools are prepared at `openFile`**, after the migrations: the writer gets every statement,
the reader only the reads.

**A statement runs on a transaction or on the pool, and the call site says which.** This is the
shape the conversion starts from, and `execer` cannot express it: `Tx.StmtContext` is a method on
`*sql.Tx`, which `*sql.DB` does not have, while `execer` exists exactly so one helper serves both
— `setCookie` takes `f.db` from `SetCookie` and `tx` from `Commit`, and the janitor's
`status_history` delete runs on the pool with no transaction at all.

So the helpers hang off a small value carrying the file and an **optional** transaction:

```go
// stmts issues the file's prepared statements, on tx when there is one and on the pool
// when there is not — the same either-or execer already serves.
type stmts struct {
    f  *file
    tx *sql.Tx // nil: run on the pool
}
```

`stmts.exec(ctx, stmtSetCookie, args…)` rebinds through `Tx.StmtContext` when `tx` is non-nil and
issues the pool's own preparation when it is nil.

**The helper and the pool are independent axes.** `exec`/`query`/`queryRow` say what shape the
call has; `stmtWrites` says which pool it runs on. Reading `query` as "reads, therefore the reader"
is the trap, and [spec 5](5-deletes-return-their-uids.md) creates the one statement that springs
it: `DELETE … RETURNING uid` returns rows, so it goes through `query`, and it is unambiguously a
write. Routed by the helper it would meet `query_only` at runtime. Helper signatures take `st stmts` where they
take `ex execer` today; `execer` and `querier` stay for what is not converted.

`stmts` does not record **which pool** its transaction came from, and both kinds exist — five
write transactions on `f.db`, one read-only on `f.readDB`. Routing on `stmtWrites` alone is
correct for every combination that exists: a read id inside the read transaction takes the
reader's preparation and rebinds cleanly, a write id inside a write transaction takes the
writer's. The combination it cannot serve is the rule below's own case — a read inside a write
transaction — which needs that id prepared on the writer *as well*, and a bool cannot say so. So
adding the first such read is more than flipping a flag.

**A read that must run inside a write transaction belongs in the writer's set** — because
`Tx.StmtContext` refuses anything else: handed a statement prepared on the reader pool it fails
with `sql: Tx.Stmt: statement from different database used`. A hard error at the call, not a
silent stale read.

**No such read exists in `kubestore` today**, and the rule is here so that adding one is a
decision rather than a surprise. All five write transactions issue `ExecContext` only, and
`querier`'s one user (`getMeta`) is called from inside `KindsWithFingerprint`'s read-only
transaction on the reader pool. `recordStatusTransition` is not an exception: its `NOT EXISTS`
guard rides an `INSERT … SELECT`, so it is a write, and lands in the writer's set for that reason.

The set is closed in `(*file).close`, so a `Clear`'s fresh file gets its own — the same reasoning
that puts the janitor's start there.

## Rules

- **A statement's text lives in the table, not at the call site.** One text, one id, one
  compilation.
- **A write is never prepared on the reader**, and only step 3's test says so. The declaration
  drives its own routing, so a wrong one is invisible until it executes.
- **A read inside a write transaction runs on the writer.** `Tx.StmtContext` accepts no other
  pool's statement.
- **No text is built at runtime**, with one exemption: **PRAGMAs are not prepared**, whether or not
  their text is constant. One that carries a value cannot be — a PRAGMA takes no bound parameter,
  so `incremental_vacuum(N)` stays interpolated — and the constant ones are file lifecycle rather
  than statements: `openFile`'s `auto_vacuum` probe and repair run before the set exists, and the
  janitor's `freelist_count` belongs beside the vacuum it gates. Everything else that needs a value
  takes a placeholder, or spec 4's idiom.
- **The helper name does not pick the pool.** `stmtWrites` does. A write that returns rows still
  runs on the writer.

## Not in this pass

- **The `execer`/`querier` interfaces disappearing.** A converted helper takes `stmts` instead,
  but the interfaces stay for every helper that is not converted, and nothing forces the
  conversion to finish in one pass.
- **`sqlitemigrate`'s own statements**, which run once at open.
- **`internal/appdb`**, which issues a handful of statements at startup.

## Build order

1. The two id tables — text and writes — and the two prepare passes in `openFile`, plus
   `closeStatements` in `(*file).close`. Nothing routes through them yet.
2. `stmts` and its `exec`/`query`/`queryRow` helpers, with the transaction rebinding. The test is
   on the rebinding, which is what the helpers add: a write issued through a helper inside a
   transaction is visible within it and disappears on rollback. **Read it back through the raw
   `tx.QueryRowContext`, not through `stmts.queryRow`** — the read-back is a read inside a write
   transaction, the one combination the routing cannot serve, and through the helpers it fails
   with `statement from different database used`. The subject is the write's rebinding, not the
   read's.
3. Convert the call sites, file by file — `objects.go` first, since it is the hot path. And the
   test that pins `stmtWrites` against the statement text, since nothing else can: an id whose
   text is not a `SELECT`/`WITH` must declare a write. Exemptions carry their reason inline, so
   one added without a reason is visible.

   **`deleteObjectRow` is the one site that needs a decision rather than a translation.** It loops
   `cascadeTables`, composing four fixed texts from a constant table, and spec 5 deliberately
   leaves it a point delete — so it arrives here intact and spec 4's idiom does not apply. Four
   ids and a loop over ids, with `cascadeTables` carrying the id beside the table and column.

## Done when

A relist page compiles its statements at most once per connection instead of once per row, a
watch's re-read compiles nothing on a connection it has already used, and a statement declared on
the wrong side of the read/write split fails its test. Delete this spec when step 3 lands.
