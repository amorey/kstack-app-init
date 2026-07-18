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

import { Channel, invoke } from '@tauri-apps/api/core';
import { subscriptionExchange } from 'urql';

import { errorMessage, reportError } from '../error-bus';
import { clearStatus, markConnected, markDisconnected } from './transport-status';

// Mirrors the JSON envelopes the Rust bridge writes into the channel. `open` is
// emitted once, before any `next`, when the SSE connection is established (the
// sidecar returned 200) — the signal this file marks the op connected on.
// `complete` is the server's own graceful end (gqlgen's `event: complete`);
// `closed` is synthesized by the host when the connection ends without one
// (sidecar crash, sleep, network loss). Both reconnect, but only `closed` reports.
type SubMessage =
  | { type: 'open' }
  | { type: 'next'; payload: { data?: unknown; errors?: unknown } }
  | { type: 'error'; payload: unknown }
  | { type: 'complete' }
  | { type: 'closed' };

const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 30_000;

// Deterministic (no jitter): a desktop app has one client per subscription,
// so there's no thundering herd to spread out — predictability wins.
const backoffDelay = (attempt: number) => Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** attempt);

// Fire-and-forget drop of a host-side op. The op may already be gone (the
// transport ended on its own), so a rejection here is expected and ignored.
const dropOp = (id: number) => {
  invoke('graphql_unsubscribe', { id }).catch(() => undefined);
};

// Bridges urql's subscriptionExchange to the Tauri host's `graphql_subscribe` /
// `graphql_unsubscribe` invoke commands. Each subscription streams over its own
// host-side SSE connection to the sidecar (see
// src-tauri/src/services/sidecar/graphql/subscribe.rs); the host translates the
// SSE frames into the channel envelopes this file consumes.
//
// The transport dies silently on sleep/network loss: the SSE stream ends and the
// channel emits `complete` (or `error`), which urql would treat as the
// subscription finishing. This app's subscriptions are long-lived and never
// legitimately complete, so a transport-end while the consumer is still
// subscribed reconnects with capped exponential backoff. Only a consumer-initiated
// teardown actually ends the subscription.
//
// A reconnect is invisible to urql (the operation and its accumulated
// `useSubscription` state survive), so the connection lifecycle is published out
// of band on the transport-status side-channel (transport-status.ts), keyed by
// `operation.key`: `markConnected` on every `open` (bumping a generation),
// `markDisconnected` on every drop. useWatchSubscription reads it to expose
// `connected` and to reset its accumulator when the generation changes. The
// sidecar replays a full snapshot on every subscribe, so a delta reducer that
// starts over on the generation bump ends up holding exactly the current server
// state — an object deleted during an outage can't linger.
//
// The generation bumps on the `open` frame, not the `graphql_subscribe` ack: the
// host acks before it dials, then reports a connect failure as an `error` frame,
// so bumping on the ack would wrongly reset last-known state on every failed
// retry during an outage. `open` fires on a real connection (even an empty
// snapshot). Status notify and data frames share this one ordered channel and the
// hook's reset is generation-gated (idempotent), so no frame can outrace the reset.
//
// An `error` frame is transport-level only: gqlgen delivers GraphQL operation
// errors inside the `next` payload's `errors` field, so the host emits `error`
// only when the connection itself fails (dial/handshake/non-200). It reconnects
// rather than calling `sink.error`: such failures are almost always transient
// (the always-on engine + credential pusher re-establish auth/upstream), and
// surfacing it terminally would kill the very stream recovery flows through.
//
// Every transport end reconnects but they split on *reporting*: `closed`
// (EOF/read failure) and `error` (failed dial) report to the error bus;
// `complete` — the server's own graceful end — reconnects silently. A server can
// complete legitimately (sidecar shutdown; chat's finite stream after each
// response), so `complete` must neither banner nor end the urql operation: if the
// sidecar really went away, the reconnect's failed dial produces the reporting
// `error`; if it's chat, the consumer's pause tears the subscription down first.
export const tauriSubscriptionExchange = subscriptionExchange({
  // urql calls this with `(request, operation)`: `request` carries the printed
  // query + variables, `operation` the stable `key` we stamp transport status
  // under (the same key useWatchSubscription derives from query+variables).
  forwardSubscription(request, operation) {
    return {
      subscribe(sink) {
        const { key } = operation;
        let cancelled = false;
        let opId: number | null = null;
        let attempt = 0;
        // Whether this outage has already been reported. Separate from `attempt`
        // (the backoff level) because they reset on different proofs: backoff on
        // `open` (dial succeeded), the report gate only on `next` (a healthy data
        // frame). Otherwise a server that 200s then immediately drops would
        // re-report every cycle, flickering the auto-dismissing banner forever.
        let reportedOutage = false;
        let retryTimer: ReturnType<typeof setTimeout> | null = null;

        // `report: false` is the graceful `complete` — reconnect without
        // touching the error bus (see the frame-split note above).
        function scheduleReconnect(message: string, cause?: unknown, report = true) {
          if (cancelled) return;
          // The connection is down: publish it so the hook can render
          // "reconnecting" (it keeps its last-known data — the generation is
          // untouched until the next `open`).
          markDisconnected(key);
          // Drop the op before forgetting the id. Most terminal paths have the
          // host already remove the entry, but the malformed-frame path leaves
          // it live — graphql_unsubscribe tolerates unknown ids, so this is a
          // safe no-op either way.
          if (opId != null) dropOp(opId);
          opId = null;
          // Report only the first abnormal drop of an outage (a healthy frame
          // clears the gate). Reported off the bus directly because the result
          // never reaches the urql sink for errorReportExchange to see.
          if (report && !reportedOutage) {
            reportedOutage = true;
            reportError({ source: 'subscription', message, cause });
          }
          const delay = backoffDelay(attempt);
          attempt += 1;
          retryTimer = setTimeout(() => {
            retryTimer = null;
            if (!cancelled) connect();
          }, delay);
        }

        function connect() {
          const channel = new Channel<string>();
          let ended = false;
          // Stale = the consumer tore down, or this channel already ended
          // and a later connect() took over. Either way it must stop
          // driving the sink / scheduling another reconnect.
          const isStale = () => cancelled || ended;
          const end = (message: string, cause?: unknown, report = true) => {
            if (ended) return;
            ended = true;
            scheduleReconnect(message, cause, report);
          };

          channel.onmessage = (raw) => {
            if (isStale()) return;
            let msg: SubMessage;
            try {
              msg = JSON.parse(raw) as SubMessage;
            } catch {
              end(`malformed subscription frame: ${raw}`, raw);
              return;
            }
            if (msg.type === 'open') {
              // Connection up, snapshot about to stream. Mark connected (bumping
              // the generation) so the hook resets before the snapshot folds in.
              // Reset backoff too: the dial succeeded, so a recovery via an empty
              // snapshot (no `next` ever comes) mustn't leave the next drop capped.
              attempt = 0;
              markConnected(key);
            } else if (msg.type === 'next') {
              // A healthy data frame: the outage (if any) is over, so the next
              // abnormal drop reports as a fresh one.
              attempt = 0;
              reportedOutage = false;
              sink.next(msg.payload as never);
            } else if (msg.type === 'error') {
              const m = typeof msg.payload === 'string' ? msg.payload : JSON.stringify(msg.payload);
              end(m, msg.payload);
            } else if (msg.type === 'complete') {
              // The server's own graceful end: reconnect, silently.
              end('subscription completed by server', undefined, false);
            } else {
              // `closed` (EOF/drop synthesized by the host) — and, defensively,
              // any frame type this file doesn't know.
              end('subscription transport closed');
            }
          };

          invoke<number>('graphql_subscribe', {
            query: request.query,
            variables: request.variables ?? {},
            channel,
          })
            .then((id) => {
              // graphql_subscribe can resolve after teardown/end (it raced
              // unsubscribe() or an error): the host registered an op nobody
              // will read — drop it.
              if (isStale()) {
                dropOp(id);
              } else {
                opId = id;
              }
            })
            .catch((err: unknown) => end(errorMessage(err), err));
        }

        connect();

        return {
          unsubscribe() {
            cancelled = true;
            if (retryTimer) clearTimeout(retryTimer);
            if (opId != null) dropOp(opId);
            clearStatus(key);
          },
        };
      },
    };
  },
});
