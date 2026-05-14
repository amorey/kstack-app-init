import { useState } from 'react';
import { createRoute } from '@tanstack/react-router';
import { Cloud, Rocket, Sparkles } from 'lucide-react';
import { useClient, useMutation, useQuery, useSubscription } from 'urql';

import { Button } from '@kubetail/ui/elements/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@kubetail/ui/elements/card';
import { Input } from '@kubetail/ui/elements/input';
import { Label } from '@kubetail/ui/elements/label';
import { Separator } from '@kubetail/ui/elements/separator';
import { Switch } from '@kubetail/ui/elements/switch';

import { graphql } from '@/gql';
import { useAuth } from '@/lib/auth/auth-context';
import { Route as rootRoute } from '@/routes/__root';

import '@/index.css';

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: Main,
});

const PingQuery = graphql(`
  query Ping {
    ping
  }
`);

const TickSubscription = graphql(`
  subscription Tick {
    tick
  }
`);

const SettingsQuery = graphql(`
  query Settings {
    settings {
      placeholder
    }
  }
`);

const UpdateSettingsMutation = graphql(`
  mutation UpdateSettings($input: UpdateSettingsInput!) {
    updateSettings(input: $input) {
      placeholder
    }
  }
`);

const SettingsWatchSubscription = graphql(`
  subscription SettingsWatch {
    settingsWatch {
      placeholder
    }
  }
`);

function Main() {
  const client = useClient();
  const [name, setName] = useState('');
  const [notify, setNotify] = useState(true);
  const [ping, setPing] = useState<string>('');

  const sendPing = async () => {
    // network-only so each click bypasses the document cache and re-hits
    // the sidecar — useful for a connectivity smoke check.
    const result = await client.query(PingQuery, {}, { requestPolicy: 'network-only' }).toPromise();
    if (result.error) {
      setPing(`error: ${result.error.message}`);
    } else {
      setPing(result.data?.ping ?? '');
    }
  };

  return (
    <main className="flex min-h-svh items-center justify-center bg-background p-6">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Rocket className="h-5 w-5" />
            kstack
          </CardTitle>
          <CardDescription>A small @kubetail/ui showcase.</CardDescription>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-col gap-2">
            <Label htmlFor="name">Project name</Label>
            <Input id="name" placeholder="my-app" value={name} onChange={(e) => setName(e.currentTarget.value)} />
          </div>

          <div className="flex items-center justify-between">
            <Label htmlFor="notify">NOTIFY ME</Label>
            <Switch id="notify" checked={notify} onCheckedChange={setNotify} />
          </div>

          <Separator />

          <div className="flex justify-end gap-2">
            <Button variant="outline">Cancel</Button>
            <Button>
              <Sparkles className="h-4 w-4" />
              Create
            </Button>
          </div>

          <Separator />

          <Button variant="secondary" onClick={sendPing}>
            Ping sidecar
          </Button>
          {ping && <pre className="text-xs whitespace-pre-wrap">{ping}</pre>}

          <Separator />

          <Tick />

          <Separator />

          <CloudSyncGate />
        </CardContent>
      </Card>
    </main>
  );
}

// Gating the cloud-sync ops on auth status avoids racing the host's
// silent-restore on startup (without a bearer the cloud returns
// `unauthorized`), and unmounts the subscription on logout so the cloud
// SSE stream tears down cleanly.
function CloudSyncGate() {
  const { status, loading } = useAuth();
  if (loading) {
    return <p className="text-xs text-muted-foreground">Checking sign-in…</p>;
  }
  if (!status.authenticated) {
    return <p className="text-xs text-muted-foreground">Sign in to sync preferences.</p>;
  }
  return <CloudSyncDemo />;
}

function Tick() {
  const [{ data, error }] = useSubscription({ query: TickSubscription });
  if (error) return <p className="text-xs text-red-500">tick error: {error.message}</p>;
  return <p className="text-xs">Tick: {data?.tick ?? '…'}</p>;
}

type SaveStatus = 'idle' | 'saving' | 'saved' | 'error';

function CloudSyncDemo() {
  const [{ data, fetching, error }] = useQuery({ query: SettingsQuery });
  const [, updateSettings] = useMutation(UpdateSettingsMutation);
  const [{ data: subData }] = useSubscription({ query: SettingsWatchSubscription });

  const [draft, setDraft] = useState('');
  const [lastExternal, setLastExternal] = useState<string | undefined>(undefined);
  const [status, setStatus] = useState<SaveStatus>('idle');

  // Adopt the latest authoritative value (query result on first load,
  // subscription pushes thereafter). React-recommended "adjust state when
  // a derived input changes" pattern: compare during render and conditionally
  // setState; React replays the render and the user sees a single committed
  // update without an effect cascade.
  // https://react.dev/learn/you-might-not-need-an-effect#adjusting-some-state-when-a-prop-changes
  const external = subData?.settingsWatch?.placeholder ?? data?.settings?.placeholder;
  if (external !== undefined && external !== lastExternal && status !== 'saving') {
    setLastExternal(external);
    setDraft(external);
  }

  const save = async () => {
    setStatus('saving');
    const result = await updateSettings({ input: { placeholder: draft } });
    setStatus(result.error ? 'error' : 'saved');
  };

  return (
    <div className="flex flex-col gap-2">
      <Label htmlFor="placeholder" className="flex items-center gap-1.5">
        <Cloud className="h-3.5 w-3.5" /> Cloud-synced placeholder
      </Label>
      <Input id="placeholder" value={draft} onChange={(e) => setDraft(e.currentTarget.value)} disabled={fetching} />
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-foreground">{error ? `error: ${error.message}` : statusLabel(status)}</span>
        <Button size="sm" onClick={save} disabled={status === 'saving'}>
          Save
        </Button>
      </div>
    </div>
  );
}

function statusLabel(s: SaveStatus): string {
  switch (s) {
    case 'saving':
      return 'saving…';
    case 'saved':
      return 'saved';
    case 'error':
      return 'save failed';
    default:
      return '';
  }
}
