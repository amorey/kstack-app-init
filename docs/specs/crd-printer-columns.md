---
title: CRD printer columns
scope: sidecar + frontend
status: Planned
---

# CRD printer columns

## Goal

A custom resource's table shows the columns its CRD declares, instead of falling back to
Namespace/Name/Age.

The server ships the column **descriptors** — `{name, jsonPath, type, priority}`, static per kind
— and the webview evaluates each `jsonPath` against the object's `rawJSON`. **The sidecar computes
no cell values**, which is the whole point of shipping the native body.

## Where it stands

`object-columns.tsx` keys hand-written accessors by `gvrKey`, and carries two entries: `v1/pods`
and `apps/v1/deployments`. `columnsForKind` returns `[]` for everything else, so a CRD's table is
Namespace/Name/Age — even though the CRD declared exactly what it wanted shown.

**The read this needs is already being made.** `markCRDs`
(`sidecar/internal/clustersvc/internal/kubesync/discovery.go`) lists every
`apiextensions.k8s.io/v1` CustomResourceDefinition on each sweep, to set `IsCRD`. The printer
columns are in the same response body, unread.

## Design

### Sidecar — extend the sweep, not add one

`markCRDs` gains a second output: per served version, that version's
`spec.versions[].additionalPrinterColumns`.

**Match on the version, unlike `IsCRD`.** The existing match is `(group, plural)` with the version
deliberately dropped — one definition serves several versions and the resource is the same custom
resource at any of them. Columns are **per version**: `additionalPrinterColumns` sits inside each
`spec.versions[]` entry and two versions routinely differ. So the columns map is keyed
`(group, version, plural)` beside the existing `(group, plural)` bool, and `kind_catalog`'s
`(api_version, kind)` primary key already gives each version its own row to hold them.

**Best-effort, as `markCRDs` already is.** Listing CRDs is a cluster-scoped read RBAC commonly
denies; a refusal returns early and leaves every kind reading as built-in. Columns inherit that
exactly — no columns, universal fallback — and no verdict changes.

Carry `{name, type, jsonPath, priority}` and nothing else. `description` and `format` are
kubectl's `-o wide` help and its number formatting; neither has a consumer here, and both can be
added later without moving anything.

### Storage — a column on `kind_catalog`

`printer_columns TEXT` holding the descriptor list as JSON, NULL for a kind that declares none.

**Edit `0001_init.sql`; do not add `0002_*.sql`.** Nothing has shipped, so the initial schema is
still the whole schema. → [ADR: schema edit, not
migration](../adr/2026-08-29-schema-edit-not-migration.md), which is also where the `ALTER TABLE …
RENAME` trap is written down.

`kind_catalog` is already a rowid table *because* it holds a wide column (`schema_json`), so a
second JSON blob needs no re-argument.

**Include it in `stmtUpsertKind`'s update clause** — and note the neighbouring trap. `schema_json`
is deliberately *absent* from that clause, with a comment saying why: nothing fills it, so writing
NULL every sweep would make the column unusable to whoever eventually does. `printer_columns` is
the opposite case — the sweep is what fills it — so it must be in both the insert and the update,
or a CRD that drops a column keeps showing it forever. `catalog_test.go` pins the `schema_json`
behaviour; that test must keep passing untouched.

**A columns-only change can be swallowed in two places, and both have to be closed.**

`fingerprintOf` is the first: it is what lets `commitCatalog` skip a rewrite, so it must enumerate
the columns or a CRD whose only edit is its printer columns never reaches the table.

The watch diff is the second. `sendDiff` compares whole rows, so the JSON string sitting **on the
row** is what makes an edited CRD arrive as `Modified` at a subscriber already sitting on the
dashboard. Decoding earlier — anywhere before the diff — would close the first hole and leave the
second, and the symptom is a table that only picks up the new columns on the next reconnect. Keep
the string on the row deliberately, not incidentally.

`stmtSelectKinds` and the `KindRow` scan take the new column, **through `COALESCE(kc.printer_columns,
'')`** — scanning NULL into a `string` errors, and every built-in row is NULL, so without it `Kinds`
fails on the common path. The statement already coalesces its count, and `stmtSelectEvents`
coalesces six columns; this is that idiom, not a new one.

**`Kinds` carries the JSON as a string**; `toCachedDataKind` (`cacheddata.go`) decodes it, which is
already the projection boundary. It returns a value and has no error path — **a blob that will not
parse yields empty columns**, never a dropped frame or a new error return. The sidecar is the only
writer, so it should not happen; the branch exists so that "should not happen" has an answer and
nobody grows an error return on a projection function to give it one.

**`KindRow` must stay comparable — this is not a style choice.** `runCachedDataWatch` is
`[T comparable, F any]` and `sendDiff` decides `Modified` with `case old != r`, and the kinds watch
instantiates it at `T = kubestore.KindRow`. A `[]PrinterColumn` field on the row makes the struct
non-comparable and fails to compile at that instantiation. `ObjectRow` sets the shape already —
the body is kept off the row and read separately — there for size rather than comparability.

### Wire

```graphql
"One column a CRD asks a client to render, from its additionalPrinterColumns."
type PrinterColumn {
  "Header text, e.g. \"Replicas\"."
  name: String!
  "OpenAPI type: integer, number, string, boolean, or date. Drives rendering, not parsing."
  type: String!
  "JSONPath into the object body, e.g. \".spec.replicas\". Evaluated by the client."
  jsonPath: String!
  "kubectl hides columns above 0 unless -o wide."
  priority: Int!
}
```

**Define `clustersvc.PrinterColumn` and map it in `gqlgen.yml` beside its parent.** Every
cached-data type is mapped to its `clustersvc` counterpart; a new schema type with no mapping
autobinds into `graph/model` instead, and `clustersvc.ClusterCachedDataKind.PrinterColumns` would
then be `[]model.PrinterColumn` — `internal/` importing `graph/model`, which is the layering the
`models:` block exists to hold. Nothing under `internal/` imports it today.

`ClusterCachedDataKind.printerColumns: [PrinterColumn!]!` — empty for a built-in, and for a CRD
that declares none. It rides the **kinds** watch, not the objects watch: static per kind, where the
objects watch is per object and hot.

Note what that costs: the kinds watch re-emits a row on count churn, so the descriptors ride along
with writes that have nothing to do with them, and they are in every `Added` snapshot. Fine at
catalog scale — a few hundred kinds, a handful of small descriptors each, and only CRDs carry any —
but it is the reason to keep the payload to the four fields rather than the whole CRD version spec.

`type` is `String!` rather than an enum: the API server constrains it to five values, and it drives
rendering rather than parsing, so an unrecognized one falls through to the string case instead of
breaking the frame.

### Frontend

Three steps before any of this renders:

1. The kinds-watch operation in `src/lib/cluster-cached-data-kinds.tsx` selects `printerColumns`.
   A field the document does not name does not arrive, however complete the schema is.
2. `pnpm codegen` regenerates `src/gql/`. Its counterpart on the Go side is
   `go generate ./graph/...` (`resolver.go:3`), which regenerates `graph/generated*` — the schema
   change needs both, and only one of them is conventionally remembered.
3. **`ServerKind` (`src/lib/dashboard-resources.ts`) is hand-written, not generated**, so adding a
   required field breaks every fixture that builds one — `dashboard-resources.test.ts` and
   `dashboard-nav.test.tsx`. Mechanical, but it is the step that turns a small change into a
   sprawling diff if it is met by surprise.

`Dashboard` then passes `printerColumns` to `ObjectsTable` alongside the
`apiVersion`/`resource`/`kind`/`namespaced` it already resolves off the same record.

`columnsForKind(gvr, printerColumns)` becomes two-tier: a hand-written registry entry wins, else
descriptor-derived columns, else `[]`. **Registry first on purpose** — a hand-written accessor
exists precisely because it says something a jsonPath cannot (`podStatus` reads container state to
correct a crash-looping pod that reports `phase: Running`).

Columns slot between Name and Age, where the hand-written ones already go.

**Filter to `priority === 0`.** kubectl hides the rest behind `-o wide`; a "wide" toggle on the
table can surface them later.

Render by `type`: `date` through the same relative formatter the Age column uses, `integer` /
`number` with `tabular-nums`, `boolean` as `true`/`false`, everything else as its string. A path
that resolves to nothing, or to an object or array, renders `—` — never `[object Object]`.

### The jsonPath reader

Its own pure module (`src/lib/jsonpath.ts`) with its own test, since it is dependency-free and
the interesting cases are all in it.

Supported, which covers what CRDs write in practice:

- a leading `.`, then dotted segments — `.spec.replicas`
- bracketed integer indexes — `.status.conditions[0].type`
- bracketed quoted keys, for names containing dots — `.metadata.labels['app.kubernetes.io/name']`

**Everything else returns "unsupported", and the column renders `—`.** Do not reach for a general
JSONPath dependency to close the gap.

**The one claim not to repeat:** it is tempting to say Kubernetes only permits a simple subset
here. It does not — the API server validates `jsonPath` by parsing it with the full `k8s.io/client-go/util/jsonpath`
library, so a filter expression (`.status.conditions[?(@.type=="Ready")].status`) is a legal CRD
that we will render as `—`. That is an acceptable gap, not an impossible input, and the reader must
degrade rather than throw on it.

## Traps

- **`rawJSON` is `unknown` and may be partial.** A `Deleted` row carries last-known state. The
  reader takes `unknown` and every step must survive a missing or wrongly-typed link — the same
  rule the hand-written accessors already state.
- **A CRD can declare a column named `Name` or `Age`.** It then renders twice, beside the
  universal column. kubectl has the same behaviour; leave it, and do not add de-duplication that
  would silently drop a column the author asked for.
- **Descriptor identity is `(kind row, index)`, not the column name.** Names are not unique and
  may repeat; anything keying React children off the name needs the index.
- **A cache written before this ships has NULL `printer_columns`** for every row until the next
  sweep — up to `discoveryInterval` (10m), sooner via the store bus or a connection wake. The
  table shows universal columns in the meantime, which is what it shows today.

## Tests

**`kubesync`** (`discovery_test.go`): a CRD serving two versions with different
`additionalPrinterColumns` puts each version's columns on that version's row; a CRD with none
leaves the field empty; a list refused by RBAC leaves both `IsCRD` and the columns unset, with the
sweep's verdict unchanged.

**`kubestore`** (`catalog_test.go`): the columns round-trip through `SyncKinds` → `Kinds`; a sweep
that drops a column clears it (the update-clause trap above); the existing `schema_json` test still
passes. Plus `fingerprintOf`: two answers differing only in printer columns fingerprint apart.

**`clustersvc`** (`cacheddata_test.go`): a sweep that changes **only** the printer columns emits a
`Modified` frame on `clusterCachedDataKindsWatch`. This is the assertion that pins the row-string
choice above — without it, a later refactor that decodes earlier passes every other test.

**`src/lib/jsonpath.test.ts`**: the three supported forms; a missing path; an index past the end; a
filter expression and a wildcard, both unsupported; a path resolving to an object or an array.

**`object-columns.test.tsx`** (new file — the module has no test today): the registry wins over
descriptors for `v1/pods`; a CRD's descriptors become columns in declared order; `priority > 0` is
filtered out; each `type` renders as specified; a partial body renders `—`.

**`objects-table.test.tsx`**: a CRD kind renders its declared headers between Name and Age.

## Not in this spec

**The built-in kinds with no columns** — StatefulSet, ReplicaSet, Node, Service, Job, CronJob, PVC.
That gap is hand-written accessors, shares no mechanism with this, and can land in either order.
The one trap worth carrying forward: **DaemonSet cannot reuse the Deployment accessors** — it has
no `spec.replicas`, and its Ready is `status.numberReady`/`status.desiredNumberScheduled`. Leave
that sub-bullet in `docs/TODO.md` when this lands.

## Docs to touch when it lands

- `sidecar/CLAUDE.md`: the **"`IsCRD` comes from a CRD list, matched by (group, plural)"** bullet —
  the sweep now reads two things out of that list, and the columns match on the version the
  `IsCRD` bool deliberately drops.
- Root `CLAUDE.md`: the dashboard-nav paragraph calls kind-specific columns "a client-side registry
  — the remaining step". Narrow it to the built-ins gap.
- `docs/TODO.md`: drop the CRD bullet, keep the built-ins sub-bullet as its own item.
- Delete this spec and its index row.

## Incidental

`manager_test.go:897` hand-writes a `kind_catalog` DDL. It is asserting a `kind_counts` failure, so
it does not need the new column — worth one look while editing the schema, not a change.

`0001_init.sql:211` names `EnsureKindCatalog`, which no longer exists — `stmtResolveKindRename` is
what clears the losing row now. Fix the comment while editing that file.
