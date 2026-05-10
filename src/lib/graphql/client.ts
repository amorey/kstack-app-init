// urql client wired through the Tauri invoke bridge. Document cache is the
// default for now; revisit Graphcache once normalized caching pays for the
// schema-introspection step.
import { Client, cacheExchange, fetchExchange } from 'urql';

import { errorReportExchange } from './error-exchange';
import { invokeFetch } from './invoke-fetch';
import { tauriSubscriptionExchange } from './subscribe-exchange';

export function createGraphqlClient() {
  return new Client({
    // The URL is unused — invokeFetch ignores it — but urql requires one.
    url: 'tauri://graphql',
    // Subscription exchange must precede fetchExchange — fetchExchange
    // discards subscription operations, so anything past it never sees them.
    // errorReportExchange sits after the cache (so cache hits don't fire it)
    // and before the network exchanges (so it sees their raw errors).
    exchanges: [cacheExchange, errorReportExchange, tauriSubscriptionExchange, fetchExchange],
    fetch: invokeFetch,
    // urql defaults to GET for short queries (query in the URL); the Tauri
    // bridge only handles POST bodies, so pin every operation to POST.
    preferGetMethod: false,
  });
}
