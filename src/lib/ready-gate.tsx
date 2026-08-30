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

// Blocks the app tree until the host's `ready` command resolves (sidecar IPC
// accepting connections) — otherwise the first GraphQL call silently absorbs the
// bind-wait and failures become stuck spinners instead of an explicit retry.

import { invoke } from '@tauri-apps/api/core';
import { useCallback, useEffect, useState } from 'react';

import { Button } from '@kubetail/ui/elements/button';

import { errorMessage } from '@/lib/error-bus';

type State = { status: 'pending' } | { status: 'ready' } | { status: 'error'; message: string };

export function ReadyGate({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<State>({ status: 'pending' });
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        await invoke('ready');
        if (!cancelled) setState({ status: 'ready' });
      } catch (e) {
        if (!cancelled) setState({ status: 'error', message: errorMessage(e) });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [attempt]);

  const retry = useCallback(() => {
    setState({ status: 'pending' });
    setAttempt((n) => n + 1);
  }, []);

  if (state.status === 'ready') return <>{children}</>;

  return (
    <div
      role="status"
      aria-live="polite"
      aria-busy={state.status === 'pending'}
      className="flex min-h-[var(--app-min-h)] flex-col items-center justify-center gap-3 bg-background p-6 text-sm"
    >
      {state.status === 'pending' ? (
        <p className="text-muted-foreground">Starting…</p>
      ) : (
        <>
          <p className="text-destructive">Failed to start: {state.message}</p>
          <Button size="sm" variant="outline" onClick={retry}>
            Retry
          </Button>
        </>
      )}
    </div>
  );
}
