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

import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { stubLayout } from '@/test-utils';

import { ROW_H, ROW_H_PX, VirtualTable } from './virtual-table';
import type { VirtualColumn } from './virtual-table';

// A viewport ten rows tall.
const VIEW_H = 10 * ROW_H_PX;
stubLayout(VIEW_H);

type Row = { id: string };
const rowKey = (r: Row) => r.id;
const columns: VirtualColumn<Row>[] = [{ key: 'id', header: 'ID', className: 'w-32', cell: (r) => r.id }];
const rowsOf = (n: number, from = 0): Row[] => Array.from({ length: n }, (_, i) => ({ id: `r${from + i}` }));

function renderTable(rows: Row[]) {
  const view = render(<VirtualTable rows={rows} rowKey={rowKey} columns={columns} />);
  const scroller = view.container.firstElementChild as HTMLDivElement;
  const rerender = (next: Row[]) => view.rerender(<VirtualTable rows={next} rowKey={rowKey} columns={columns} />);
  const scrollTo = (top: number) => {
    scroller.scrollTop = top;
    fireEvent.scroll(scroller);
  };
  return { scroller, rerender, scrollTo };
}

// Data rows only: the spacer rows have no cells.
const bodyRows = () => screen.getAllByRole('row').filter((r) => r.querySelector('td'));
const spacers = () => Array.from(document.querySelectorAll<HTMLTableRowElement>('tbody > tr:not(:has(td))'));

describe('VirtualTable', () => {
  it('renders only the rows in the window, between two spacer rows', () => {
    renderTable(rowsOf(10_000));
    const rows = bodyRows();
    expect(rows.length).toBeGreaterThan(10);
    expect(rows.length).toBeLessThan(100);
    expect(rows[0]).toHaveTextContent('r0');
    rows.forEach((r) => expect(r).toHaveClass(ROW_H));
    const [top, bottom] = spacers();
    expect(top.style.height).toBe('0px');
    expect(bottom.style.height).toBe(`${(10_000 - rows.length) * ROW_H_PX}px`);
  });

  it('lays the table out fixed, with each column carrying its class on header and cells', () => {
    renderTable(rowsOf(3));
    expect(screen.getByRole('table')).toHaveClass('table-fixed');
    expect(screen.getByRole('columnheader', { name: 'ID' })).toHaveClass('w-32');
    expect(screen.getByText('r1')).toHaveClass('w-32');
  });

  // Home/End/PageUp/PageDown are the browser's on a focused overflow container.
  it('makes the scroller focusable', () => {
    const { scroller } = renderTable(rowsOf(3));
    expect(scroller).toHaveAttribute('tabindex', '0');
  });

  it('moves the window with the scroll offset', () => {
    const { scrollTo } = renderTable(rowsOf(10_000));
    scrollTo(5_000 * ROW_H_PX);
    const rows = bodyRows();
    expect(rows[0]).toHaveTextContent('r4990'); // 10 rows of overscan above the viewport
    expect(rows.at(-1)).toHaveTextContent('r5019');
    const [top, bottom] = spacers();
    expect(top.style.height).toBe(`${4_990 * ROW_H_PX}px`);
    expect(bottom.style.height).toBe(`${(10_000 - 5_020) * ROW_H_PX}px`);
  });

  describe('when the rows change under the viewport', () => {
    // The user is reading r500 at the top of a 1,000-row list.
    const parked = () => {
      const view = renderTable(rowsOf(1_000));
      view.scrollTo(500 * ROW_H_PX);
      return view;
    };

    it('holds the view when rows are inserted above it', () => {
      const { scroller, rerender } = parked();
      rerender([...rowsOf(50, 10_000), ...rowsOf(1_000)]);
      expect(scroller.scrollTop).toBe(550 * ROW_H_PX);
    });

    it('holds the view when rows are deleted above it', () => {
      const { scroller, rerender } = parked();
      rerender(rowsOf(1_000).slice(100));
      expect(scroller.scrollTop).toBe(400 * ROW_H_PX);
    });

    // The rows below it stay where they are; a row from above fills the slot it left.
    it('holds the rows below a deleted first visible row', () => {
      const { scroller, rerender } = parked();
      rerender(rowsOf(1_000).filter((r) => r.id !== 'r500'));
      expect(scroller.scrollTop).toBe(499 * ROW_H_PX);
    });

    // A re-fired event moves from wherever it is to the top of a newest-first list.
    it('holds the view when the first visible row moves to the top', () => {
      const { scroller, rerender } = parked();
      const rows = rowsOf(1_000);
      rerender([rows[500], ...rows.filter((r) => r.id !== 'r500')]);
      expect(scroller.scrollTop).toBe(500 * ROW_H_PX);
    });

    it('follows new rows when parked at the top', () => {
      const { scroller, rerender } = renderTable(rowsOf(1_000));
      rerender([{ id: 'fresh' }, ...rowsOf(1_000)]);
      expect(scroller.scrollTop).toBe(0);
      expect(screen.getByText('fresh')).toBeInTheDocument();
    });

    // Chromium would stack its own anchoring on the correction above; WebKit has none.
    it('turns native scroll anchoring off', () => {
      const { scroller } = renderTable(rowsOf(3));
      expect(scroller).toHaveClass('[overflow-anchor:none]');
    });
  });
});
