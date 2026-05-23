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

// Mirrors the JSON envelopes the Rust bridge writes into the channel.
type SubMessage =
  | { type: 'next'; payload: { data?: unknown; errors?: unknown } }
  | { type: 'error'; payload: unknown }
  | { type: 'complete' };

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
// `graphql_subscribe` / `graphql_unsubscribe` invoke commands. All
// subscriptions multiplex over a single host-side WebSocket (see
// src-tauri/src/services/sidecar/subscribe.rs for the rationale).
//
// The transport dies silently on sleep/network loss: the host WS ends and
// the channel emits `complete` (or `error`). urql would treat that as the
// subscription finishing — collapsing it with no reconnect. This app's
// subscriptions (settingsWatch/syncStatusWatch) are long-lived and never
// legitimately complete, so a transport-end while the consumer is still
// subscribed is reconnected with capped exponential backoff. Only a
// consumer-initiated teardown actually ends the subscription.
//
// A server-sent `error` frame is treated the same as a transport drop
// (reconnect, not `sink.error`) on purpose: the only subscriptions here
// are the engine-backed settings/sync watches, and a failure is almost
// always transient (the always-on engine + credential pusher re-establish
// auth/upstream). Surfacing it terminally would also kill the very stream
// that recovery flows through. A genuinely permanent error (schema/
// permission) therefore degrades to a quiet capped retry rather than a
// hard failure — acceptable, since these two internal watches have no
// terminal UI and the alternative is a dead, unrecoverable subscription.
export const tauriSubscriptionExchange = subscriptionExchange({
  forwardSubscription(operation) {
    return {
      subscribe(sink) {
        let cancelled = false;
        let opId: number | null = null;
        let attempt = 0;
        let retryTimer: ReturnType<typeof setTimeout> | null = null;

        function scheduleReconnect(message: string, cause?: unknown) {
          if (cancelled) return;
          // Always dropOp before forgetting the id: most terminal paths
          // (server-sent `complete`/`error`, fan_out_complete on WS drop)
          // have the host already remove the entry, but the malformed-
          // frame path leaves it live — graphql_unsubscribe is documented
          // as tolerant of unknown ids, so this is a safe no-op when the
          // host already cleared things up.
          if (opId != null) dropOp(opId);
          opId = null;
          // Report only the first drop of an outage. Backoff caps at 30s
          // and the banner auto-dismisses after 5s, so reporting every
          // retry would make it flicker forever while down. A healthy
          // frame resets `attempt`, so a fresh outage reports again. The
          // banner is driven off the bus directly because the result no
          // longer reaches the urql sink (errorReportExchange can't see it).
          if (attempt === 0) reportError({ source: 'subscription', message, cause });
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
          const end = (message: string, cause?: unknown) => {
            if (ended) return;
            ended = true;
            scheduleReconnect(message, cause);
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
            if (msg.type === 'next') {
              attempt = 0; // a good frame proves the connection is healthy
              sink.next(msg.payload as never);
            } else if (msg.type === 'error') {
              const m = typeof msg.payload === 'string' ? msg.payload : JSON.stringify(msg.payload);
              end(m, msg.payload);
            } else {
              end('subscription transport closed');
            }
          };

          invoke<number>('graphql_subscribe', {
            query: operation.query,
            variables: operation.variables ?? {},
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
          },
        };
      },
    };
  },
});
