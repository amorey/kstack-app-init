// urql exchange that taps the result stream and forwards any operation
// errors (network or GraphQL) to the global error bus. Pure observer — does
// not transform results — so it can sit anywhere between cache and network
// without affecting cache hits or mutations.
//
// Built on `mapExchange` (urql 4+); replaces the older `errorExchange`.

import { mapExchange } from 'urql';

import { reportError, type AppErrorSource } from '../error-bus';

export const errorReportExchange = mapExchange({
  onError(error, operation) {
    const source: AppErrorSource = operation.kind === 'subscription' ? 'subscription' : 'graphql';
    // Prefer the network message when present — it tells the user the
    // sidecar is unreachable rather than that "an error occurred."
    const message = error.networkError?.message ?? error.message;
    reportError({ source, message, cause: error });
  },
});
