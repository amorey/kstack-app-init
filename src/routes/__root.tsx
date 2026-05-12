import { lazy, Suspense } from 'react';
import { createRootRoute, Outlet } from '@tanstack/react-router';
import { Provider as UrqlProvider } from 'urql';

import { AuthProvider } from '@/lib/auth/auth-context';
import { ProfileMenu } from '@/lib/auth/profile-menu';
import { ConnectionStatus } from '@/lib/connection-status';
import { ErrorBoundary } from '@/lib/error-boundary';
import { createGraphqlClient } from '@/lib/graphql/client';

const TanStackRouterDevtools =
  import.meta.env.VITE_ROUTER_DEVTOOLS === 'true'
    ? lazy(() =>
        import('@tanstack/react-router-devtools').then((res) => ({
          default: res.TanStackRouterDevtools,
        })),
      )
    : () => null;

// Single client for the app's lifetime. Created at module load so tests
// that mount via routeTree get the Provider transitively without extra wiring.
const gqlClient = createGraphqlClient();

export const Route = createRootRoute({
  notFoundComponent: NotFound,
  component: RootComponent,
});

export function NotFound() {
  return (
    <div className="container">
      <h1>404</h1>
      <p>Page not found.</p>
    </div>
  );
}

function RootComponent() {
  return (
    <ErrorBoundary>
      <AuthProvider>
        <UrqlProvider value={gqlClient}>
          <div className="fixed right-3 top-3 z-50">
            <ProfileMenu />
          </div>
          <ConnectionStatus />
          <Outlet />
          <Suspense>
            <TanStackRouterDevtools />
          </Suspense>
        </UrqlProvider>
      </AuthProvider>
    </ErrorBoundary>
  );
}
