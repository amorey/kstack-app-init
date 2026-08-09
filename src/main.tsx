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

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { RouterProvider, createRouter } from '@tanstack/react-router';

import { routeTree } from '@/routeTree';
import { isLinux } from '@/lib/platform';

import '@/index.css';
import 'unfonts.css';

// Linux only: the window is frameless *and* transparent, so tag the document to
// make its background transparent (macOS/Windows stay opaque, full-bleed).
// See docs/adr/2026-08-09-per-platform-window-chrome.md
if (isLinux()) document.documentElement.classList.add('frameless');

// The `.dark` class is applied pre-paint by the `index.html` inline script;
// `ThemeProvider` re-applies on mount and owns changes thereafter.
// See docs/adr/2026-08-09-first-paint-theming.md

const router = createRouter({ routeTree });

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}

createRoot(document.getElementById('root') as HTMLElement).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
);
