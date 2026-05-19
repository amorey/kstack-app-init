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

// Coerce an unknown thrown/rejected value into an Error. Pass-through if
// it already is one (preserves the original stack); otherwise wrap
// String(x). `catch` and rejected-promise values are `unknown` in TS, so
// this is the one place that narrowing lives.
export function toError(x: unknown): Error {
  return x instanceof Error ? x : new Error(String(x));
}

// The human-readable message of an unknown thrown value — an Error's
// `.message` (not its `"Error: …"` toString), or String(x) otherwise.
export function errorMessage(x: unknown): string {
  return toError(x).message;
}

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
