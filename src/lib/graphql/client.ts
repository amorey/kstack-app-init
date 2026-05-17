// urql client wired through the Tauri invoke bridge. Document cache is the
// default for now; revisit Graphcache once normalized caching pays for the
// schema-introspection step.
import { retryExchange } from '@urql/exchange-retry';
import { Client, cacheExchange, fetchExchange } from 'urql';

import { errorReportExchange } from './error-exchange';
import { invokeFetch } from './invoke-fetch';
import { tauriSubscriptionExchange } from './subscribe-exchange';

// Retries only operations that failed with a `networkError` (the package
// default `retryIf`) — transport blips while the local sidecar restarts.
// GraphQL errors are deterministic and must NOT be retried. Bounded so a
// hard-down sidecar fails fast rather than hanging the UI; the always-on
// engine + subscription reconnect own long-term recovery. `randomDelay`
// de-correlates the several queries a route fires at once so they don't
// re-hit the sidecar in lockstep on its restart — unlike subscribe-exchange,
// which deliberately omits jitter (a single long-lived client, no herd).
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
    // Subscription exchange must precede fetchExchange — fetchExchange
    // discards subscription operations, so anything past it never sees them.
    // errorReportExchange sits after the cache (so cache hits don't fire it)
    // and before retry (so it reports only the final, post-retry error, not
    // every attempt). Retry never fights the subscription auto-reconnect:
    // subscribe-exchange never surfaces an error result (it reconnects
    // internally), so retryExchange has no subscription failure to retry.
    exchanges: [cacheExchange, errorReportExchange, networkRetryExchange, tauriSubscriptionExchange, fetchExchange],
    fetch: invokeFetch,
    // urql defaults to GET for short queries (query in the URL); the Tauri
    // bridge only handles POST bodies, so pin every operation to POST.
    preferGetMethod: false,
  });
}
