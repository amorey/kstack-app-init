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

import { AuthProvider } from '@/lib/auth';
import { ErrorBoundary } from '@/lib/error-boundary';
import { createGraphqlClient } from '@/lib/graphql/client';
import { ReadyGate } from '@/lib/ready-gate';
import { ClustersProvider } from '@/lib/clusters';
import { KubeConfigProvider } from '@/lib/kube-config';
import { WindowFrame } from '@/components/widgets/window-frame';

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

// Providers only — the visual shell lives in the `_app` layout route
// (`AppLayout`). `Outlet` resolves to that layout, which renders the sidebar
// and the routed page. `WindowFrame` wraps everything (including the loading and
// error states) so the frameless Linux/Windows window gets its border and outer
// shadow; it's a passthrough on macOS.
function RootComponent() {
  return (
    <WindowFrame>
      <ErrorBoundary>
        <ReadyGate>
          <UrqlProvider value={gqlClient}>
            <AuthProvider>
              <ClustersProvider>
                <KubeConfigProvider>
                  <Outlet />
                  <Suspense>
                    <TanStackRouterDevtools />
                  </Suspense>
                </KubeConfigProvider>
              </ClustersProvider>
            </AuthProvider>
          </UrqlProvider>
        </ReadyGate>
      </ErrorBoundary>
    </WindowFrame>
  );
}
