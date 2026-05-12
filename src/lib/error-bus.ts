// Tiny in-process pub/sub for surfacing transient runtime errors — network
// failures, GraphQL errors, dropped subscriptions, render-time exceptions
// caught by the error boundary — to a single global UI surface (see
// `ConnectionStatus`). Decoupling the producer from the surface means step
// 5 (Sentry) can tap the same bus without touching producers.
//
// Backed by `EventTarget` so listeners are GC-friendly and order is FIFO.

export type AppErrorSource = 'graphql' | 'subscription' | 'network' | 'render' | 'auth';

export type AppError = {
  source: AppErrorSource;
  message: string;
  // Original Error / CombinedError, kept for upstream reporters that want
  // stack traces. Not rendered in the UI.
  cause?: unknown;
};

const target = new EventTarget();
const ERROR_EVENT = 'app-error';

export function reportError(error: AppError): void {
  target.dispatchEvent(new CustomEvent(ERROR_EVENT, { detail: error }));
}

export function onError(handler: (error: AppError) => void): () => void {
  const listener = (e: Event) => handler((e as CustomEvent<AppError>).detail);
  target.addEventListener(ERROR_EVENT, listener);
  return () => target.removeEventListener(ERROR_EVENT, listener);
}
