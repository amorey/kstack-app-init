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

import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { Route } from './chat';

// The route stays mounted and deep-linkable while the assistant is unbuilt, so the
// sidebar's mode switch keeps working. Rendering the component bare is the
// assertion that it asks for nothing — no GraphQL provider, no transport.
function renderChat() {
  const Chat = Route.options.component!;
  return render(<Chat />);
}

describe('chat route', () => {
  it('says the assistant is unavailable', () => {
    renderChat();
    expect(screen.getByText("Chat isn't available yet.")).toBeInTheDocument();
  });

  it('takes no input', () => {
    renderChat();
    expect(screen.getByPlaceholderText('Message…')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Send' })).toBeDisabled();
  });
});
