import { useState } from 'react';
import { createRoute } from '@tanstack/react-router';
import { Rocket, Sparkles } from 'lucide-react';

import { Button } from '@kubetail/ui/elements/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@kubetail/ui/elements/card';
import { Input } from '@kubetail/ui/elements/input';
import { Label } from '@kubetail/ui/elements/label';
import { Separator } from '@kubetail/ui/elements/separator';
import { Switch } from '@kubetail/ui/elements/switch';

import { Route as rootRoute } from '@/routes/__root';

import '@/index.css';

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: Main,
});

function Main() {
  const [name, setName] = useState('');
  const [notify, setNotify] = useState(true);

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
            <Label htmlFor="notify">Notify me</Label>
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
        </CardContent>
      </Card>
    </main>
  );
}
