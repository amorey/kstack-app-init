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

import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import type { ClusterCachedDataObject } from '@/lib/cluster-cached-data-objects';
import { columnsForKind } from './object-columns';
import type { ObjectColumn } from './object-columns';

const widgetGVR = { apiVersion: 'example.com/v1', resource: 'widgets' };

function objectWith(rawJSON: unknown): ClusterCachedDataObject {
  return {
    uid: 'u-1',
    namespace: 'default',
    name: 'widget-1',
    creationTimestamp: '',
    rawJSON,
  } as ClusterCachedDataObject;
}

// Render a cell to text, since a column's cell may be an element (a date renders a time-ago).
function cellText(column: ObjectColumn, o: ClusterCachedDataObject): string {
  const { container } = render(<>{column.cell(o)}</>);
  return container.textContent ?? '';
}

describe('columnsForKind', () => {
  it('gives an unregistered kind with no descriptors nothing', () => {
    expect(columnsForKind(widgetGVR, [])).toEqual([]);
  });

  // A hand-written accessor exists because it says what a jsonPath cannot — podStatus reads
  // container state to correct a crash-looping pod that reports phase Running.
  it('prefers the hand-written registry over descriptors', () => {
    const columns = columnsForKind({ apiVersion: 'v1', resource: 'pods' }, [
      { name: 'Phase', type: 'string', jsonPath: '.status.phase', priority: 0 },
    ]);

    expect(columns.map((c) => c.header)).toEqual(['Ready', 'Status', 'Restarts']);
  });

  it('builds columns from descriptors in declared order', () => {
    const columns = columnsForKind(widgetGVR, [
      { name: 'Replicas', type: 'integer', jsonPath: '.spec.replicas', priority: 0 },
      { name: 'Phase', type: 'string', jsonPath: '.status.phase', priority: 0 },
    ]);

    expect(columns.map((c) => c.header)).toEqual(['Replicas', 'Phase']);
    const o = objectWith({ spec: { replicas: 3 }, status: { phase: 'Running' } });
    expect(cellText(columns[0], o)).toBe('3');
    expect(cellText(columns[1], o)).toBe('Running');
  });

  // kubectl hides these behind -o wide; a table with no wide toggle should not show them.
  it('drops columns above priority 0', () => {
    const columns = columnsForKind(widgetGVR, [
      { name: 'Phase', type: 'string', jsonPath: '.status.phase', priority: 0 },
      { name: 'Node', type: 'string', jsonPath: '.spec.nodeName', priority: 1 },
    ]);

    expect(columns.map((c) => c.header)).toEqual(['Phase']);
  });

  it('renders a boolean and a missing value', () => {
    const columns = columnsForKind(widgetGVR, [
      { name: 'Paused', type: 'boolean', jsonPath: '.spec.paused', priority: 0 },
      { name: 'Missing', type: 'string', jsonPath: '.spec.nope', priority: 0 },
    ]);

    const o = objectWith({ spec: { paused: false } });
    expect(cellText(columns[0], o)).toBe('false');
    expect(cellText(columns[1], o)).toBe('—');
  });

  // A path resolving to a container renders nothing rather than "[object Object]".
  it('renders a non-scalar as absent', () => {
    const columns = columnsForKind(widgetGVR, [{ name: 'Spec', type: 'string', jsonPath: '.spec', priority: 0 }]);

    expect(cellText(columns[0], objectWith({ spec: { replicas: 3 } }))).toBe('—');
  });

  it('survives a partial body, which a Deleted row carries', () => {
    const columns = columnsForKind(widgetGVR, [
      { name: 'Replicas', type: 'integer', jsonPath: '.spec.replicas', priority: 0 },
    ]);

    expect(cellText(columns[0], objectWith(undefined))).toBe('—');
  });
});
