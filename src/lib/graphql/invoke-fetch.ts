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

import { invoke } from '@tauri-apps/api/core';

import { toError } from '../error-bus';

const JSON_HEADERS = { 'content-type': 'application/json' } as const;

// What the Rust `graphql_query` command returns. See
// src-tauri/src/services/sidecar/graphql/query.rs (GraphqlResponse). The status
// is carried separately so we can preserve it on the synthesized
// Response — urql treats 4xx/5xx as non-retryable server errors and
// network errors as retryable. Collapsing them (e.g. status: 200
// regardless) would have retryExchange hammer a permanent 4xx forever.
type GraphqlResponse = { status: number; body: string };

// Adapter that lets urql's stock fetchExchange talk to the Tauri sidecar
// over the host's `graphql_query` invoke handler instead of `window.fetch`.
// The sidecar listens on a Unix domain socket, so the webview can't dial
// it directly — the Rust host bridges the request and hands back the raw
// JSON response body alongside the real HTTP status.
export const invokeFetch: typeof fetch = async (_input, init) => {
  // urql's fetchExchange always sends a string body for POST. If anything
  // else shows up we forward an empty body and let the sidecar reject it.
  const body = typeof init?.body === 'string' ? init.body : '';
  try {
    const { status, body: text } = await invoke<GraphqlResponse>('graphql_query', { body });
    return new Response(text, { status, headers: JSON_HEADERS });
  } catch (err) {
    // A transport failure (sidecar unreachable / restarting) is a *network*
    // error, not a GraphQL one. Throw so urql's fetchExchange yields a
    // CombinedError with a real `networkError`: the UI can tell "sidecar
    // down" from a server error, and retryExchange (network-errors-only)
    // can retry it. A fabricated 500 envelope would defeat both.
    throw toError(err);
  }
};
