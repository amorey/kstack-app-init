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

import { useCallback, useMemo, useSyncExternalStore } from 'react';
import { createRequest, useSubscription } from 'urql';
import type { AnyVariables, UseSubscriptionArgs, UseSubscriptionResponse } from 'urql';

import { getStatus, subscribeStatus } from './transport-status';

// Reduced value tagged with the connection generation it folded under; comparing
// tags is how a reconnect's snapshot replaces prior state without a synthetic frame.
type Generational<Result> = { generation: number; result: Result };

type Base<Result, Variables extends AnyVariables> = UseSubscriptionResponse<Result | undefined, Variables>;

// urql's response shape plus `connected`; `data` is undefined until the current
// connection's first frame.
export type UseWatchSubscriptionResponse<Result, Variables extends AnyVariables> = [
  Base<Result, Variables>[0] & { connected: boolean },
  Base<Result, Variables>[1],
];

// App-wide wrapper over urql's `useSubscription`: reads the transport-status
// side-channel to expose `connected` and reset the accumulator across reconnects.
// The reset is generation-gated two ways off the same counter (reducer folds onto
// undefined on tag mismatch; exposed `data` masked while the tag is stale), so
// it's order-independent between side-channel notify and sink frame.
// See docs/adr/2026-08-09-transport-status-generation.md
export function useWatchSubscription<Data, Result, Variables extends AnyVariables = AnyVariables>(
  args: UseSubscriptionArgs<Variables, Data>,
  reduce: (prev: Result | undefined, data: Data) => Result,
): UseWatchSubscriptionResponse<Result, Variables> {
  // The op key the exchange stamps transport status under (urql derives the
  // same key from query+variables).
  const key = useMemo(() => createRequest(args.query, args.variables).key, [args.query, args.variables]);

  const status = useSyncExternalStore(
    useCallback((onChange) => subscribeStatus(key, onChange), [key]),
    () => getStatus(key),
  );

  const [result, executeSubscription] = useSubscription<Data, Generational<Result>, Variables>(args, (prev, data) => {
    const { generation } = getStatus(key);
    // Frame from a newer connection than the accumulator ⇒ fold onto a clean slate.
    const base = prev && prev.generation === generation ? prev.result : undefined;
    return { generation, result: reduce(base, data) };
  });

  // Mask a prior-connection accumulator (snapshot not yet folded, or empty and
  // never will be) back to "no data yet".
  const accumulated = result.data;
  const data = accumulated && accumulated.generation === status.generation ? accumulated.result : undefined;

  return [{ ...result, data, connected: status.connected }, executeSubscription];
}

// A watch's rendering state. `connecting` covers everything before the collection
// is known — transport still dialing, or its snapshot still arriving — so a
// consumer renders an empty state only from `live`/`reconnecting`, where empty
// means empty. `reconnecting` is a drop with last-known data held.
export type WatchPhase = 'connecting' | 'reconnecting' | 'live';

// `synced` means the snapshot's Bookmark has landed. Deriving it from "any data
// yet" instead would report a populated collection as empty for the whole time the
// server spends listing it — `connected` flips on the transport's open frame, which
// precedes the first row.
export function watchPhase(synced: boolean, connected: boolean): WatchPhase {
  if (!synced) return 'connecting';
  return connected ? 'live' : 'reconnecting';
}
