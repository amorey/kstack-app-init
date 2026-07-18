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

import { retryExchange } from '@urql/exchange-retry';
import { Client, cacheExchange, fetchExchange } from 'urql';

import { errorReportExchange } from './error-exchange';
import { invokeFetch } from './invoke-fetch';
import { tauriSubscriptionExchange } from './subscribe-exchange';

// Retries only `networkError` failures (the package default `retryIf`) — transport
// blips while the local sidecar restarts. GraphQL errors are deterministic and must
// NOT be retried. Bounded so a hard-down sidecar fails fast rather than hanging the
// UI; the always-on engine + subscription reconnect own long-term recovery.
// `randomDelay` de-correlates the several queries a route fires at once so they
// don't re-hit the sidecar in lockstep — unlike subscribe-exchange, which omits
// jitter (a single long-lived client, no herd).
const networkRetryExchange = retryExchange({
  initialDelayMs: 500,
  maxDelayMs: 5_000,
  maxNumberAttempts: 3,
  randomDelay: true,
});

export function createGraphqlClient() {
  return new Client({
    // The URL is unused — invokeFetch ignores it — but urql requires one.
    url: 'tauri://graphql',
    // Order matters: the subscription exchange must precede fetchExchange (which
    // discards subscription operations). errorReportExchange sits after the cache
    // (so cache hits don't fire it) and before retry (so it reports only the final,
    // post-retry error). Retry never fights the subscription auto-reconnect —
    // subscribe-exchange never surfaces an error result to retry.
    exchanges: [cacheExchange, errorReportExchange, networkRetryExchange, tauriSubscriptionExchange, fetchExchange],
    fetch: invokeFetch,
    // urql defaults to GET for short queries (query in the URL); the Tauri
    // bridge only handles POST bodies, so pin every operation to POST.
    preferGetMethod: false,
  });
}
