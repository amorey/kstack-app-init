---
title: The tables render only the rows on screen
scope: frontend
status: Planned
---

# The tables render only the rows on screen

**Needs:** nothing. No sidecar change, no schema change, no change to the watches these views read.
**Hands on:** one render seam (`VirtualTable`) that the backend paging work in `docs/TODO.md`
("Paged object watches") can feed differently without touching how rows render.

## Goal

Scroll through a kind of any size without the browser holding a DOM row per object.

Two views render every row their hook returns, and both are unbounded:

| View | Hook | Size |
| --- | --- | --- |
| `objects-table.tsx` | `useClusterCachedDataObjects` | a 10,000-Pod kind is 10,000 `<tr>`s |
| `events-table.tsx` | `useClusterCachedDataEvents` | an event-storming namespace |

Both re-render on every delta frame. The data is already complete, live and sorted; the render is
the cost this spec removes.

**The backend is deliberately untouched.** The sidecar still reads the whole kind per burst and
ships every `rawJSON` body on subscribe. That is the memory and first-paint cost, and it is a
separate line of work. This spec buys scroll and re-render, nothing else, and it stays in place when
that work lands.

## No dependency

Every row is the same height (below), so the visible window is arithmetic:

```
first = floor(scrollTop / ROW_H_PX) - OVERSCAN
last  = ceil((scrollTop + clientHeight) / ROW_H_PX) + OVERSCAN
```

clamped to `[0, rows.length)`. A virtualization library earns its keep measuring rows of varying
height; with uniform rows it would add a dependency and its own scroll bookkeeping around these two
lines. `VirtualTable` owns them instead.

## Part 1 — `VirtualTable`

**One component, `src/components/widgets/virtual-table.tsx`**, that both tables render through.

```ts
type VirtualColumn<T> = {
  key: string;
  header: string;
  cell: (row: T) => ReactNode;
  className?: string; // applied to the header and every cell: width, alignment, tabular-nums
};

type VirtualTableProps<T> = {
  rows: readonly T[];
  rowKey: (row: T) => string; // module-level function — it keys a per-frame index map
  columns: VirtualColumn<T>[];
};
```

`VirtualColumn` has the same shape as `ObjectColumn` in `object-columns.tsx`, on purpose: the
objects table builds its `columns` by concatenating the universal columns (Namespace, Name, Age)
with `columnsForKind(...)` and nothing is mapped. Per-cell styling that the header should not share
(`text-muted-foreground` on a namespace, `font-medium` on a name) goes on an element inside `cell`.

**`rowKey` must be stable for the same object across frames.** `uid` is. It now does more than
React reconciliation: the scroll anchoring below is keyed on it.

### State

Two numbers from the scroll element, `scrollTop` (from `onScroll`) and `clientHeight` (from a
`ResizeObserver`), held in state. Everything else — the window, the spacer heights, the anchor
correction — is derived from those and `rows`.

### Three constraints

**1. `table-fixed`, and the free-text columns truncate.**
An auto-layout table sizes its columns from the rows in the DOM, and a virtualized table has a
different set of rows in the DOM on every scroll, so widths would jump while scrolling. Under
`table-layout: fixed` the widths come from the header row, which is always present, so they hold.
Columns without a width share whatever the sized columns leave; that is how Name (objects) and
Message (events) get the slack, as they do today.

Under fixed layout a cell can no longer widen its column, and `TableCell` is `whitespace-nowrap`, so
free text would spill across the neighbouring cells. Name, Message and the events Object column get
`truncate` and a `title` carrying the full text. (The Object cell's `max-w-0` goes — it only exists
to make an auto-layout cell truncate.) The registry's extra columns and a CRD's printer columns
carry no width today; give the descriptor columns a default in `object-columns.tsx` so a CRD's
columns don't split the slack equally with Name.

**2. One row height, pinned in CSS, known to the component.**
Every row is already one line tall (`whitespace-nowrap` on every cell). Pin it, so a font or padding
change cannot silently drift it: `virtual-table.tsx` exports the pair and applies the class to every
`<tr>` it renders, including the spacers.

```ts
export const ROW_H = 'h-9';
export const ROW_H_PX = 36;
```

**Read `ROW_H_PX` off DevTools, not arithmetic.** `TableRow` carries `border-b` and Tailwind
collapses table borders, so the pinned row's `getBoundingClientRect().height` may not be the `h-9`
value. A wrong constant shows up as rows creeping out of position further down a long list — the
error accumulates once per row — so check it once against a rendered row at implementation time.
Drop the `align-top` classes both tables carry; they only ever mattered for wrapped cells.

**3. The scroll element is ours.**
`@kubetail/ui`'s `Table` wraps the `<table>` in `<div class="relative w-full overflow-x-auto">` and
forwards no ref to it. `VirtualTable`'s root is the scroller — `min-h-0 flex-1 overflow-auto` — and
the library's wrapper is neutralised with `containerClassName="overflow-x-visible"` (same
tailwind-merge group as `overflow-x-auto`, so it replaces it; `overflow-visible` would not). Both
tables also wrap `<Table>` in a redundant `<div className="overflow-x-auto">` today; remove it.

The scroller also gets `tabIndex={0}` and a visible focus ring, so Home/End/PageUp/PageDown work —
the browser scrolls a focused overflow container on those keys, and the window follows the scroll
event like any other. Nothing to build. The `<thead>` is `sticky top-0 bg-background`.

`ObjectsTable`/`EventsTable` keep their three early returns — not synced, connecting, empty — as
the plain `<p>`/spinner they are today. Only the live branch changes, to
`<VirtualTable rows=… rowKey=… columns=… />`. The route supplies the column; the table fills it.

### Rows, not absolute positioning

The window is real `<tr>`s between two spacer rows:

```tsx
<tbody>
  <tr style={{ height: first * ROW_H_PX }} />
  {rows.slice(first, last).map((row) => <TableRow key={rowKey(row)} className={ROW_H}>…</TableRow>)}
  <tr style={{ height: (rows.length - last) * ROW_H_PX }} />
</tbody>
```

Native `<table>` semantics, the existing `@kubetail/ui` components, and accessibility all survive;
none do under the absolutely-positioned rows most virtualization examples use.

## The rows change while you are looking at them

These are live delta watches. Both hooks rebuild and re-sort the array on **every frame**, so rows
shift index under a scroll offset that is just a number of pixels. Insert one row above the
viewport and every row the user is reading slides down by `ROW_H_PX`, having done nothing.

Nothing in the browser saves this. WebKit — the webview on macOS and Linux — has no scroll
anchoring. Chromium (Windows) does, and it must be **turned off** on the scroller with
`[overflow-anchor:none]`, or it and the correction below both fire and the view moves twice.

The two tables sit at opposite ends of the problem:

- **Events sort newest-first** by `lastSeen`. Every new event lands at index 0 and shifts the whole
  list. A re-fired event (a `Modified` bumping `count`/`lastSeen`) *moves* — from wherever it was
  to index 0.
- **Objects sort by `(namespace, name)`**, which never changes for a `uid`, so a row is only ever
  inserted or removed. An insert lands anywhere and shifts what follows it.

### Keep the rows on screen where they are

After every commit, `VirtualTable` remembers `{ key, index }` for each rendered row from the first
visible one down (`index >= floor(scrollTop / ROW_H_PX)`). On the next commit it looks each
remembered key up in a `Map<key, index>` built from the new `rows` (a `useMemo` on `rows`; O(N) per
frame, already dwarfed by the hook's sort) and takes the **smallest** shift among the rows that
survive:

```ts
// Rows the user is reading move together when something is inserted or removed above them.
// A row that moved on its own — a re-fired event jumping to the top — is the outlier.
shift = min over surviving rows of |newIndex − oldIndex|   (signed value kept)
scroller.scrollTop += shift * ROW_H_PX
```

That one rule covers every case:

| Change | Visible rows' shifts | Result |
| --- | --- | --- |
| Insert above the viewport | all `+1` | view holds |
| Delete above the viewport | all `−1` | view holds |
| Insert or delete below | all `0` | nothing |
| The first visible row is deleted | it is missing; the rest `0` | view holds |
| A visible event re-fires to the top | that row `−i`; the rest `+1` | view holds |
| 50 rows prepended in one frame | all `+50` | view holds |

The correction runs in a `useLayoutEffect` with no deps, so it lands before paint. Setting
`scrollTop` raises a `scroll` event, and the window recomputes from it as it would from the user
scrolling; for the one frame in between, the rows that fill the gap come from `OVERSCAN` — so a
burst larger than `OVERSCAN` rows in one frame shows as a blank flash, not a wrong position.
`OVERSCAN` is 10; today's watches deliver one object per frame.

**At the top, follow instead.** When `scrollTop < ROW_H_PX` the user is parked at the top and
new rows should appear as they arrive — the behaviour a log tail has. Skip the correction. It is
`<`, not `=== 0`: an exact comparison flips to anchored on the first pixel of momentum scroll.
Nothing is needed at the bottom — a row appended below the viewport shifts nothing, and when
`rows` shrinks under the offset the browser clamps `scrollTop` to the new end and fires `scroll`.

A table that changes kind (`ObjectsTable` on a resource switch) passes through `connecting` and
unmounts `VirtualTable`, so the next kind starts at the top. No key is needed on the route.

### What this does not fix

The hooks still copy and re-sort the whole array per frame, before `VirtualTable` sees anything.
Virtualization removes the render cost, not the fold cost; that is the separate `docs/TODO.md` item
("The delta folds are O(N²) over an on-subscribe burst"). A 10,000-row kind under heavy churn will
still burn CPU in the hook.

What it fixes incidentally: every row mounts a `ReactTimeAgo` with its own timer. Ten thousand
rows is ten thousand live timers; virtualized, it is the ~30 on screen.

## Part 2 — the layout has to bound the scroll

The one change outside the two tables, and the riskiest.

A scroller needs a **definite** height. Today nothing has one: the chain is `SidebarProvider`
(`min-h-(--app-min-h)`, `app-sidebar.tsx`) → `SidebarInset` → `<main>` (`flex min-h-(--app-min-h)
flex-col`, `app-layout.tsx`) → the route's `<section>`. `min-h-` grows with content, so a spacer row
would just make the document taller: every row rendered, the page scrolling — the bug, not the fix.

**The change:** the two `min-h-(--app-min-h)` become `min-h-0 h-(--app-min-h)`; `<main>` gains
`overflow-hidden`; the dashboard `<section>` becomes `flex min-h-0 flex-1 flex-col` (keeping
`min-w-0 p-6`) so `VirtualTable`'s `min-h-0 flex-1 overflow-auto` root fills it. `min-h-0` is
required at every flex level — without it a flex child refuses to shrink below its content and the
chain is unbounded again.

**Why `min-h-0` and not just the swap.** `SidebarProvider`'s root hardcodes `min-h-svh`;
`app-sidebar.tsx` overrides it today only because `min-h-(--app-min-h)` is in the same
tailwind-merge group. `h-*` is a different group, so swapping alone leaves `min-h-svh` in force. On
macOS that is invisible — `--app-min-h` is `100svh`, they agree. On Linux `--app-min-h` is `100%`
of the `inset-4` `WindowFrame`, two rems shorter than `100svh`, so `min-height` would win and the
chain would be taller than the frame clipping it — on the one platform a macOS dev machine cannot
see.

**A percentage height needs definite ancestors.** On Linux `--app-min-h` is `100%`, so the chain is
definite only while every DOM ancestor is. It holds today: the provider wrapper's only DOM
ancestor is `WindowFrame`'s `fixed inset-4` box (the providers between them render no elements),
and `<main>` is a stretched flex child of `SidebarInset`. One wrapper `<div>` breaks it. `min-h`
forgave that; `h` does not.

**Chat:** `/chat` already scrolls its own message list (`chat.tsx`, `flex-1 overflow-y-auto`).
What it lacks is `min-h-0` on that chain. Without it the list refuses to shrink, `<main>`'s new
`overflow-hidden` clips it, and the composer goes off-screen. Add `min-h-0` to `CenteredColumn`
(whose only caller is `chat.tsx`) and to the list div. That is the whole blast radius: `chat.tsx`
is the only other route under `_app`.

*Considered and rejected:* virtualizing against document scroll, which would need no layout change.
It keeps the page-scrolling model — sidebar and table header scroll away with the rows — which is
wrong for a desktop window. Recorded so the layout question is not silently reopened.

## What does not change

- **Sorting stays client-side over the full set.** No sort UI is added; the order is the hook's.
- **The four states** — no active cache, `connecting`, empty, live — render exactly as now.
- **Row keys** stay `uid`. **The hooks** are untouched.
- **The resource nav is not virtualized.** `dashboard-resource-nav.tsx` is a recursive tree and
  would need flattening first — real work, for ~40 rows at rest.

## Tests

Co-located, no wall-clock waits.

`virtual-table.test.tsx`:

- 10,000 rows put far fewer than 10,000 `<tr>`s in the DOM, and the ones present are the visible
  window plus overscan.
- The table carries `table-fixed` and every column renders its declared class. **Not** "widths are
  identical before and after a scroll": jsdom gives `<th>`s no measured width, so that reads zero
  both times and passes against the very bug it names.
- Scrolling (set `scrollTop`, dispatch `scroll`) moves the window; the spacer heights are
  `first * ROW_H_PX` and `(count − last) * ROW_H_PX`.
- **Rows inserted above the viewport do not move the view**: scroll to the middle, prepend 50 rows,
  assert the same key is first visible and `scrollTop` grew by `50 * ROW_H_PX`.
- **Deleting the first visible row** holds the view (its successor is now first visible at the
  same `scrollTop`).
- **A visible row moving to index 0** (an event re-firing) holds the view.
- **At the top, the view follows**: `scrollTop` 0, prepend a row, `scrollTop` is still 0 and the
  new row is rendered.
- The scroller has `overflow-anchor: none`.

**jsdom has no layout**: every element reports zero height and `ResizeObserver` does not exist, so
a virtualized table renders no rows at all. The harness defines `clientHeight` on the scroll element
and installs a minimal `ResizeObserver` (calls back once on `observe`). **That stub goes in
`src/test-utils.tsx`, beside `mockTauriCore()`** — all three suites need it. `objects-table.test.tsx`
and `events-table.test.tsx` do **not** keep passing unchanged: every existing test that asserts on
a row (`getByText('my-pod')`) renders nothing until the stub is applied; with it, their assertions
stand as written.

## When it lands

Fold into the root `CLAUDE.md`'s dashboard section: `VirtualTable` as the way tables render; the
pinned row height (`ROW_H`/`ROW_H_PX` as one fact, read off a rendered row); the smallest-shift
scroll rule and `overflow-anchor: none` (and why an offset alone means nothing under a live watch);
the bounded-height chain — `min-h-0` beside `h-(--app-min-h)` rather than a class swap, and that
Linux's percentage height is definite only while its ancestors are. Then delete this spec.
