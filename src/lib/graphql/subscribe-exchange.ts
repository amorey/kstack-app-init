// Bridges urql's subscriptionExchange to the Tauri host's
// `graphql_subscribe` / `graphql_unsubscribe` invoke commands. Each
// subscription gets its own WebSocket on the Rust side (see
// src-tauri/src/sidecar/subscribe.rs for the rationale).
import { Channel, invoke } from '@tauri-apps/api/core';
import { subscriptionExchange } from 'urql';

// Mirrors the JSON envelopes the Rust bridge writes into the channel.
type SubMessage =
  | { type: 'next'; payload: { data?: unknown; errors?: unknown } }
  | { type: 'error'; payload: unknown }
  | { type: 'complete' };

export const tauriSubscriptionExchange = subscriptionExchange({
  forwardSubscription(operation) {
    return {
      subscribe(sink) {
        const channel = new Channel<string>();
        let opId: number | null = null;
        let cancelled = false;

        channel.onmessage = (raw) => {
          let msg: SubMessage;
          try {
            msg = JSON.parse(raw) as SubMessage;
          } catch {
            sink.error(new Error(`malformed subscription frame: ${raw}`));
            return;
          }
          if (msg.type === 'next') {
            sink.next(msg.payload as never);
          } else if (msg.type === 'error') {
            sink.error(new Error(typeof msg.payload === 'string' ? msg.payload : JSON.stringify(msg.payload)));
          } else {
            sink.complete();
          }
        };

        invoke<number>('graphql_subscribe', {
          query: operation.query,
          variables: operation.variables ?? {},
          channel,
        })
          .then((id) => {
            // If the subscriber tore down before the host registered us,
            // immediately ask the host to drop the operation.
            if (cancelled) {
              invoke('graphql_unsubscribe', { id }).catch(() => undefined);
            } else {
              opId = id;
            }
          })
          .catch((err: unknown) => sink.error(new Error(String(err))));

        return {
          unsubscribe() {
            cancelled = true;
            if (opId != null) invoke('graphql_unsubscribe', { id: opId }).catch(() => undefined);
          },
        };
      },
    };
  },
});
