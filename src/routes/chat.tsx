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

import { createRoute } from '@tanstack/react-router';

import { Button } from '@kubetail/ui/elements/button';
import { Input } from '@kubetail/ui/elements/input';

import { CenteredColumn } from '@/components/widgets/centered-column';
import { Route as appRoute } from '@/routes/_app';

export const Route = createRoute({
  getParentRoute: () => appRoute,
  path: '/chat',
  component: Chat,
});

// A placeholder until the assistant is built. The route stays mounted so it remains
// deep-linkable and the sidebar's mode switch keeps working; the composer is inert
// because there is nothing behind it.
function Chat() {
  return (
    <CenteredColumn>
      <div className="flex-1 overflow-y-auto">
        <p className="text-sm text-muted-foreground">Chat isn&apos;t available yet.</p>
      </div>
      <form className="flex gap-2" onSubmit={(e) => e.preventDefault()}>
        <Input value="" readOnly placeholder="Message…" disabled className="bg-sidebar" />
        <Button type="submit" disabled>
          Send
        </Button>
      </form>
    </CenteredColumn>
  );
}
