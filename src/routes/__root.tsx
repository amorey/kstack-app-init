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

import { lazy, Suspense } from 'react';
import { createRootRoute, Outlet } from '@tanstack/react-router';
import { Provider as UrqlProvider } from 'urql';

import { SessionProvider } from '@/lib/auth';
import { ProfileMenu } from '@/components/widgets/profile-menu';
import { ConnectionStatus } from '@/lib/connection-status';
import { ErrorBoundary } from '@/lib/error-boundary';
import { createGraphqlClient } from '@/lib/graphql/client';
import { SyncHealthBadge } from '@/components/widgets/sync-health-badge';
import { SyncStatusProvider } from '@/lib/sync-status';

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
      <SessionProvider>
        <UrqlProvider value={gqlClient}>
          <SyncStatusProvider>
            <div className="fixed right-3 top-3 z-50 flex items-center gap-2">
              <SyncHealthBadge />
              <ProfileMenu />
            </div>
            <ConnectionStatus />
            <Outlet />
            <Suspense>
              <TanStackRouterDevtools />
            </Suspense>
          </SyncStatusProvider>
        </UrqlProvider>
      </SessionProvider>
    </ErrorBoundary>
  );
}
