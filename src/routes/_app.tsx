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

// Pathless layout route for the main window's shell. It adds no path segment;
// it just wraps its children (chat, dashboard) in `AppLayout` so they share the
// sidebar. Secondary windows (log tail, exec) will nest under their own
// sidebar-less layout route instead.
//
// It also owns the `kubeContext` search param: the window's active kubeconfig
// context (see `@/lib/active-context`). `retainSearchParams` carries it across
// the chat<->dashboard navigation, so the choice is shared by both modes and
// stays deep-linkable, without any provider.
import { createRoute, retainSearchParams } from '@tanstack/react-router';

import { AppLayout } from '@/layouts/app-layout';
import { Route as rootRoute } from '@/routes/__root';

type AppSearch = { kubeContext?: string };

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  id: 'app',
  validateSearch: (search: Record<string, unknown>): AppSearch =>
    typeof search.kubeContext === 'string' ? { kubeContext: search.kubeContext } : {},
  search: { middlewares: [retainSearchParams(['kubeContext'])] },
  component: AppLayout,
});
