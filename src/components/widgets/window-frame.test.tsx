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

import { render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { MAC_USER_AGENT, NON_MAC_USER_AGENT, restoreUserAgent, setUserAgent } from '@/test-utils';

import { WindowFrame } from './window-frame';

beforeEach(() => {
  setUserAgent(NON_MAC_USER_AGENT);
});

afterEach(() => {
  restoreUserAgent();
});

describe('WindowFrame', () => {
  it('wraps children in the bordered frame on Linux/Windows', () => {
    render(
      <WindowFrame>
        <div>content</div>
      </WindowFrame>,
    );
    const frame = screen.getByTestId('window-frame');
    expect(frame).toContainElement(screen.getByText('content'));
  });

  it('renders children full-bleed on macOS (native decorations own the frame)', () => {
    setUserAgent(MAC_USER_AGENT);
    render(
      <WindowFrame>
        <div>content</div>
      </WindowFrame>,
    );
    expect(screen.queryByTestId('window-frame')).not.toBeInTheDocument();
    expect(screen.getByText('content')).toBeInTheDocument();
  });
});
