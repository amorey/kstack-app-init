// Copyright 2026 The Kstack Authors
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

// Pathless layout route: wraps chat/dashboard in `AppLayout` so they share the
// sidebar (secondary windows will nest under their own layout route). Owns the
// `kubeContext` search param; `retainSearchParams` carries it across the mode
// switch. See docs/adr/2026-08-09-url-params-as-window-state.md
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
