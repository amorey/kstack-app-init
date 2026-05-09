// urql client wired through the Tauri invoke bridge. Document cache is the
// default for now; revisit Graphcache once normalized caching pays for the
// schema-introspection step.
import { Client, cacheExchange, fetchExchange } from 'urql';

import { invokeFetch } from './invoke-fetch';

export function createGraphqlClient() {
  return new Client({
    // The URL is unused — invokeFetch ignores it — but urql requires one.
    url: 'tauri://graphql',
    exchanges: [cacheExchange, fetchExchange],
    fetch: invokeFetch,
    // urql defaults to GET for short queries (query in the URL); the Tauri
    // bridge only handles POST bodies, so pin every operation to POST.
    preferGetMethod: false,
  });
}
