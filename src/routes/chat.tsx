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

import { useRef, useState } from 'react';
import { createRoute } from '@tanstack/react-router';

import { Button } from '@kubetail/ui/elements/button';
import { Input } from '@kubetail/ui/elements/input';

import { graphql } from '@/gql';
import { CenteredColumn } from '@/components/widgets/centered-column';
import { useWatchSubscription } from '@/lib/graphql/use-watch-subscription';
import { Route as appRoute } from '@/routes/_app';

export const Route = createRoute({
  getParentRoute: () => appRoute,
  path: '/chat',
  component: Chat,
});

const ChatStreamSubscription = graphql(`
  subscription ChatStream($input: ChatInput!) {
    chatStream(input: $input) {
      delta
      done
    }
  }
`);

type Msg = { id: string; from: 'user' | 'assistant'; content: string };

function Chat() {
  const [messages, setMessages] = useState<Msg[]>([]);
  const [draft, setDraft] = useState('');
  // While `pending` is set the subscription is active and `streamed` holds the
  // in-flight assistant text. On `done` we move the text into `messages` and clear
  // `pending` in one handler call — React 18 batches both setStates into one
  // commit, so the streaming and finalized bubbles never co-exist for a frame.
  const [pending, setPending] = useState<Msg[] | null>(null);
  const [streamed, setStreamed] = useState('');
  // Guards against duplicate `done` deliveries (StrictMode double-mount in
  // dev, late frames after the subscription is already paused, etc.).
  const finishedRef = useRef(true);

  useWatchSubscription(
    {
      query: ChatStreamSubscription,
      variables: {
        input: { messages: (pending ?? []).map((m) => ({ role: m.from, content: m.content })) },
      },
      pause: pending === null,
    },
    // On a transport reconnect useWatchSubscription starts accumulation over —
    // the re-established stream re-runs the request and streams from the top.
    (prev: string | undefined, data) => {
      const chunk = data.chatStream;
      const next = (prev ?? '') + chunk.delta;
      if (chunk.done) {
        if (finishedRef.current) return next;
        finishedRef.current = true;
        setMessages((m) => [...m, { id: crypto.randomUUID(), from: 'assistant', content: next }]);
        setStreamed('');
        setPending(null);
      } else {
        setStreamed(next);
      }
      return next;
    },
  );

  const send = () => {
    const text = draft.trim();
    if (!text || pending) return;
    const next: Msg[] = [...messages, { id: crypto.randomUUID(), from: 'user', content: text }];
    finishedRef.current = false;
    setMessages(next);
    setDraft('');
    setStreamed('');
    setPending(next);
  };

  return (
    <CenteredColumn>
      <div className="flex-1 space-y-3 overflow-y-auto">
        {messages.length === 0 && !pending && <p className="text-sm text-muted-foreground">Say hi to start a chat.</p>}
        {messages.map((m) => (
          <Bubble key={m.id} from={m.from}>
            {m.content}
          </Bubble>
        ))}
        {pending && <Bubble from="assistant">{streamed || '…'}</Bubble>}
      </div>
      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          send();
        }}
      >
        <Input
          value={draft}
          onChange={(e) => setDraft(e.currentTarget.value)}
          placeholder="Message…"
          disabled={!!pending}
          className="bg-sidebar"
        />
        <Button type="submit" disabled={!!pending || !draft.trim()}>
          Send
        </Button>
      </form>
    </CenteredColumn>
  );
}

function Bubble({ from, children }: { from: 'user' | 'assistant'; children: React.ReactNode }) {
  return (
    <div
      className={
        from === 'user'
          ? 'ml-auto max-w-[80%] rounded-lg bg-primary px-3 py-2 text-sm text-primary-foreground whitespace-pre-wrap'
          : 'mr-auto max-w-[80%] rounded-lg bg-muted px-3 py-2 text-sm whitespace-pre-wrap'
      }
    >
      {children}
    </div>
  );
}
