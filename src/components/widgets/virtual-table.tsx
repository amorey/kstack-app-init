// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A table that renders only the rows on screen. Every row is `ROW_H` tall, so the
// visible window is arithmetic over `scrollTop`, and the rows are real `<tr>`s between
// two spacer rows that stand in for everything above and below.
//
// The rows are a live watch: they shift index under the scroll offset on every frame.
// After each commit the table remembers which rows were on screen and, on the next,
// moves `scrollTop` by the smallest shift any of them made, so what the user is reading
// stays put. Native scroll anchoring is off (`overflow-anchor: none`) — Chromium's would
// stack on top of this correction; WebKit has none.
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { ReactNode } from 'react';

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@kubetail/ui/elements/table';

// One fact, two forms. `ROW_H_PX` is the pinned row's rendered height — `TableRow` has
// `border-b` under collapsed table borders, so it is read off a rendered row, not derived.
export const ROW_H = 'h-9';
export const ROW_H_PX = 36;

// Rows rendered beyond each edge of the viewport. Also covers the one frame between a
// scroll correction below and the `scroll` event that recomputes the window from it.
const OVERSCAN = 10;

export type VirtualColumn<T> = {
  key: string;
  header: string;
  cell: (row: T) => ReactNode;
  // Applied to the header and every cell: width, alignment, tabular-nums.
  className?: string;
};

type VirtualTableProps<T> = {
  rows: readonly T[];
  // Stable across frames for the same object (`uid`); the scroll correction is keyed on it.
  // Pass a module-level function — it keys a per-frame index map.
  rowKey: (row: T) => string;
  columns: VirtualColumn<T>[];
};

type Seen = { key: string; index: number };

export function VirtualTable<T>({ rows, rowKey, columns }: VirtualTableProps<T>) {
  const scrollerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewHeight, setViewHeight] = useState(0);

  useEffect(() => {
    const el = scrollerRef.current!;
    const observer = new ResizeObserver(() => setViewHeight(el.clientHeight));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const first = Math.max(0, Math.floor(scrollTop / ROW_H_PX) - OVERSCAN);
  const last = Math.min(rows.length, Math.ceil((scrollTop + viewHeight) / ROW_H_PX) + OVERSCAN);

  const indexByKey = useMemo(() => new Map(rows.map((row, i) => [rowKey(row), i])), [rows, rowKey]);

  // The rows on screen at the last commit, from the first visible one down.
  const seenRef = useRef<Seen[]>([]);
  useLayoutEffect(() => {
    const el = scrollerRef.current!;
    // Within a row of the top the user is parked there: let new rows arrive in view.
    if (el.scrollTop >= ROW_H_PX) {
      // Rows on screen move together when something is inserted or removed above them;
      // a row that moved on its own (an event re-fired to the top) is the outlier.
      let shift = 0;
      let best = Infinity;
      seenRef.current.forEach(({ key, index }) => {
        const now = indexByKey.get(key);
        if (now === undefined || Math.abs(now - index) >= best) return;
        shift = now - index;
        best = Math.abs(shift);
      });
      el.scrollTop += shift * ROW_H_PX;
    }
    const firstVisible = Math.max(first, Math.floor(el.scrollTop / ROW_H_PX));
    seenRef.current = [];
    for (let i = firstVisible; i < last; i += 1) seenRef.current.push({ key: rowKey(rows[i]), index: i });
  });

  return (
    <div
      ref={scrollerRef}
      // Focusable so Home/End/PageUp/PageDown reach the scroll region.
      // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
      tabIndex={0}
      onScroll={(e) => setScrollTop(e.currentTarget.scrollTop)}
      className="flex-1 overflow-auto [overflow-anchor:none] focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
    >
      {/* The library's wrapper is a scroll container of its own; this one is the scroller. */}
      <Table className="table-fixed" containerClassName="overflow-x-visible">
        <TableHeader className="sticky top-0 bg-background">
          <TableRow>
            {columns.map((c) => (
              <TableHead key={c.key} className={c.className}>
                {c.header}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          <tr aria-hidden style={{ height: first * ROW_H_PX }} />
          {rows.slice(first, last).map((row) => (
            <TableRow key={rowKey(row)} className={ROW_H}>
              {columns.map((c) => (
                <TableCell key={c.key} className={c.className}>
                  {c.cell(row)}
                </TableCell>
              ))}
            </TableRow>
          ))}
          <tr aria-hidden style={{ height: (rows.length - last) * ROW_H_PX }} />
        </TableBody>
      </Table>
    </div>
  );
}
