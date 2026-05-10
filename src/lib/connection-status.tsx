// Transient banner driven by the error bus. Shown when a GraphQL operation,
// subscription, or network call fails; auto-dismisses after a few seconds so
// a brief reconnect doesn't leave a stale banner around.
//
// Deliberately minimal styling — no toast lib for one surface. Revisit if
// more transient UI shows up (then a `sonner`-style provider earns its keep).

import { useEffect, useState } from 'react';

import { onError, type AppError } from './error-bus';

const DISMISS_AFTER_MS = 5_000;

export function ConnectionStatus() {
  const [latest, setLatest] = useState<AppError | null>(null);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    const unsubscribe = onError((err) => {
      setLatest(err);
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => setLatest(null), DISMISS_AFTER_MS);
    });
    return () => {
      unsubscribe();
      if (timer) clearTimeout(timer);
    };
  }, []);

  if (!latest) return null;

  // Subscriptions get a softer message because the urql subscriptionExchange
  // will re-issue the operation when components re-mount; framing it as
  // "reconnecting" is honest about what the user can expect to happen next.
  const text = latest.source === 'subscription' ? 'Subscription dropped — reconnecting…' : `Error: ${latest.message}`;

  return (
    <div
      role="status"
      aria-live="polite"
      className="fixed inset-x-0 top-0 z-50 mx-auto mt-2 w-fit rounded bg-destructive px-3 py-1.5 text-xs text-destructive-foreground shadow"
    >
      {text}
    </div>
  );
}
