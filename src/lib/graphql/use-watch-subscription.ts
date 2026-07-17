// Copyright 2026 The Kubetail Authors
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

// The reduced value tagged with the connection generation it was folded under
// (see transport-status.ts). `generation` bumps once per (re)established
// connection, so comparing tags is how a reconnect's snapshot replaces
// prior-connection state without a synthetic frame on the data channel.
type Generational<Result> = { generation: number; result: Result };

type Base<Result, Variables extends AnyVariables> = UseSubscriptionResponse<Result | undefined, Variables>;

// Same shape as urql's response, except the first tuple element gains
// `connected` — up now? — alongside the usual result fields (`data` is
// `Result | undefined`, undefined until the current connection's first frame).
export type UseWatchSubscriptionResponse<Result, Variables extends AnyVariables> = [
  Base<Result, Variables>[0] & { connected: boolean },
  Base<Result, Variables>[1],
];

// The app-wide replacement for urql's `useSubscription`. It owns the
// transport-reset + connection-status contract: the exchange publishes each
// subscription's connection lifecycle on the transport-status side-channel
// (keyed by the operation key), and this hook renders it as `connected` and,
// crucially, resets its accumulator across a reconnect *without* a synthetic
// data frame — so `data` is only ever real GraphQL data.
//
// The reset is generation-gated two ways, both keyed off the same generation so
// they're idempotent regardless of whether the side-channel notify or the sink
// frame lands first:
//   - the reducer folds a frame onto `undefined` (fresh) whenever the live
//     generation differs from the accumulator's tag — the non-empty reconnect,
//   - and the exposed `data` is masked to `undefined` whenever the accumulator's
//     tag is stale — the empty-snapshot reconnect and the window between a
//     connection's `open` and its first frame (so a spinner keyed on
//     `data === undefined` distinguishes "connecting" from "connected, empty").
//
// Every subscription must go through this hook; reducers then hold only domain
// logic.
export function useWatchSubscription<Data, Result, Variables extends AnyVariables = AnyVariables>(
  args: UseSubscriptionArgs<Variables, Data>,
  reduce: (prev: Result | undefined, data: Data) => Result,
): UseWatchSubscriptionResponse<Result, Variables> {
  // The op key the exchange stamps transport status under — the same key urql
  // derives from query+variables for this operation. The reducer reads status
  // live (per frame) off it, so the value must be stable across renders (it is —
  // same query+variables ⇒ same key).
  const key = useMemo(() => createRequest(args.query, args.variables).key, [args.query, args.variables]);

  const status = useSyncExternalStore(
    useCallback((onChange) => subscribeStatus(key, onChange), [key]),
    () => getStatus(key),
  );

  const [result, executeSubscription] = useSubscription<Data, Generational<Result>, Variables>(args, (prev, data) => {
    const { generation } = getStatus(key);
    // Fold onto a clean slate when this frame belongs to a newer connection than
    // whatever we accumulated last — the reconnect reset, no frame required.
    const base = prev && prev.generation === generation ? prev.result : undefined;
    return { generation, result: reduce(base, data) };
  });

  // Mask an accumulator left over from a prior connection (a reconnect whose
  // snapshot hasn't folded yet, or an empty one that never will) back to
  // "no data yet".
  const accumulated = result.data;
  const data = accumulated && accumulated.generation === status.generation ? accumulated.result : undefined;

  return [{ ...result, data, connected: status.connected }, executeSubscription];
}
