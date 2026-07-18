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

// Mirrors the JSON envelopes the Rust bridge writes into the channel. `open`
// is emitted once, before any `next`, when the SSE connection is actually
// established (the sidecar returned 200) — the signal this file marks the op
// connected on (see below).
// `complete` is the server's own graceful end (gqlgen's `event: complete`);
// `closed` is synthesized by the host when the connection ends without one
// (sidecar crash, sleep, network loss). Both reconnect — see below — but only
// `closed` reports.
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

// Bridges urql's subscriptionExchange to the Tauri host's
// `graphql_subscribe` / `graphql_unsubscribe` invoke commands. Each
// subscription is streamed over its own host-side SSE connection to the
// sidecar (see src-tauri/src/services/sidecar/graphql/subscribe.rs); the host
// translates the SSE frames into the `next`/`complete`/`error` channel
// envelopes this file consumes.
//
// The transport dies silently on sleep/network loss: the host's SSE stream
// ends and the channel emits `complete` (or `error`). urql would treat that as
// the subscription finishing — collapsing it with no reconnect. This app's
// subscriptions (settingsWatch/syncStatusWatch) are long-lived and never
// legitimately complete, so a transport-end while the consumer is still
// subscribed is reconnected with capped exponential backoff. Only a
// consumer-initiated teardown actually ends the subscription.
//
// Because a reconnect is invisible to urql (the operation and its accumulated
// `useSubscription` state survive), the connection lifecycle is published out
// of band on the transport-status side-channel (transport-status.ts), keyed by
// `operation.key`: `markConnected` on every `open` (which also bumps a
// generation), `markDisconnected` on every drop. useWatchSubscription reads it
// to (1) expose `connected`, and (2) reset its accumulator when the generation
// changes — so the reset signal no longer rides the data channel as a synthetic
// frame, and `data` is only ever real GraphQL data. The sidecar replays a full
// snapshot on every subscribe, so a delta reducer that starts over on the
// generation bump ends up holding exactly the current server state — an object
// deleted during an outage, which the replay simply omits, can't linger.
//
// The generation bumps on the host's `open` frame, which arrives once per
// connection ahead of the snapshot: it fires on a real connection (even an
// empty snapshot, which replays no `next`), but *not* on a failed dial — the
// host acks `graphql_subscribe` before it dials, then reports a connect failure
// as an `error` frame, so bumping on the ack would wrongly reset last-known
// state on every failed retry during an outage. Both the status notify and the
// data frames are driven off this one ordered channel, and the hook's reset is
// generation-gated (idempotent), so no frame can outrace the reset.
//
// An `error` frame is now transport-level only: gqlgen's SSE transport delivers
// GraphQL operation errors inside the `next` payload's `errors` field, so the
// host emits `error` solely when the connection itself fails (dial/handshake/
// non-200). It's treated the same as a transport drop (reconnect, not
// `sink.error`) on purpose: the only subscriptions here are the engine-backed
// settings/sync watches, and such a failure is almost always transient (the
// always-on engine + credential pusher re-establish auth/upstream). Surfacing
// it terminally would also kill the very stream that recovery flows through.
//
// Every transport end reconnects, but they split on *reporting*. `closed`
// (EOF/read failure) and `error` (failed dial) are abnormal and report to the
// error bus; `complete` — the server's own graceful end — reconnects silently.
// A server can complete a long-lived watch legitimately (sidecar shutdown, and
// chat's finite stream completes after every response), so `complete` must
// neither banner nor end the urql operation: if the sidecar really went away,
// the reconnect's failed dial produces the `error` that reports; if it's chat,
// the consumer's pause tears the subscription down before the retry fires.
export const tauriSubscriptionExchange = subscriptionExchange({
  // urql calls this with `(request, operation)`: `request` carries the printed
  // query + variables, `operation` carries the stable `key` we stamp transport
  // status under (the same key the client's operation — and useWatchSubscription
  // — derive from query+variables).
  forwardSubscription(request, operation) {
    return {
      subscribe(sink) {
        const { key } = operation;
        let cancelled = false;
        let opId: number | null = null;
        let attempt = 0;
        // Whether the current outage has already been reported. Separate from
        // `attempt` (the backoff level) because the two reset on different
        // proofs: backoff resets on `open` (the dial succeeded, so the next
        // drop is a fresh outage that deserves a prompt retry), while the
        // report gate resets only on `next` (a healthy *data* frame) — a
        // flapping server that 200s and immediately drops would otherwise
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
          // Always dropOp before forgetting the id: most terminal paths
          // (server-sent `complete`/`error`, fan_out_complete on WS drop)
          // have the host already remove the entry, but the malformed-
          // frame path leaves it live — graphql_unsubscribe is documented
          // as tolerant of unknown ids, so this is a safe no-op when the
          // host already cleared things up.
          if (opId != null) dropOp(opId);
          opId = null;
          // Report only the first abnormal drop of an outage. Backoff caps at
          // 30s and the banner auto-dismisses after 5s, so reporting every
          // retry would make it flicker forever while down. A healthy frame
          // clears `reportedOutage`, so a fresh outage reports again. The
          // banner is driven off the bus directly because the result no
          // longer reaches the urql sink (errorReportExchange can't see it).
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
              // The connection is up and the snapshot is about to stream. Mark
              // the op connected (bumping its generation), so the hook resets
              // its accumulator before the reconnect's snapshot folds in. The
              // dial succeeded, so backoff starts over — without this, an
              // outage recovered by an *empty*-snapshot connection (no `next`
              // ever comes) would leave the next drop at the 30s cap.
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
              // graphql_subscribe can resolve after the connection was
              // already torn down/ended (it raced unsubscribe() or an
              // error): the host registered an op nobody will read — drop it.
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
