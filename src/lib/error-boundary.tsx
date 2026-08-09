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

// Top-level error boundary; the reload button is cheaper than resetting arbitrary
// state. Async errors (handlers, effects, subscriptions) aren't caught here — they
// flow through the error bus.

import { Component, type ErrorInfo, type ReactNode } from 'react';

import { Button } from '@kubetail/ui/elements/button';

import { reportError } from '@/lib/error-bus';

type Props = { children: ReactNode };
type State = { error: Error | null };

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Mirror to the bus so future reporters see render errors; componentStack is
    // logged only, never surfaced.
    console.error('ErrorBoundary caught:', error, info.componentStack);
    reportError({ source: 'render', message: error.message, cause: error });
  }

  render(): ReactNode {
    const { error } = this.state;
    const { children } = this.props;
    if (!error) return children;

    return (
      <div className="flex min-h-[var(--app-min-h)] items-center justify-center bg-background p-6">
        <div className="max-w-lg space-y-4 text-center">
          <h1 className="text-xl font-semibold">Something went wrong</h1>
          <pre className="whitespace-pre-wrap rounded bg-muted p-3 text-left text-xs text-muted-foreground">
            {error.message}
          </pre>
          <Button onClick={() => window.location.reload()}>Reload window</Button>
        </div>
      </div>
    );
  }
}
