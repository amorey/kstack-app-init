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

// The app's settings dialog. Opened from the sidebar account menu via
// `openDialog('settings')` and rendered above the sidebar by `AppDialogs`, so it
// outlives the sidebar card unmounting on auto-collapse. Each setting is a `Field`
// row inside a `FieldGroup`; today the only one is Appearance (the theme picker).
import { Monitor, Moon, Sun } from 'lucide-react';
import type { ComponentType } from 'react';

import { Field, FieldContent, FieldDescription, FieldGroup, FieldLabel } from '@kubetail/ui/elements/field';
import { Tabs, TabsList, TabsTrigger } from '@kubetail/ui/elements/tabs';

import { Dialog } from '@/components/widgets/dialog';
import { type AppDialogProps } from '@/lib/dialog';
import { type ColorSchemePreference, useColorScheme } from '@/lib/theme';

// The three color-scheme choices, in reading order. "System" leads because it's
// the default (follow the OS).
const COLOR_SCHEME_OPTIONS: { value: ColorSchemePreference; label: string; icon: ComponentType }[] = [
  { value: 'system', label: 'System', icon: Monitor },
  { value: 'light', label: 'Light', icon: Sun },
  { value: 'dark', label: 'Dark', icon: Moon },
];

// Segmented single-choice control for the color scheme: the library's `Tabs`
// strip used as a picker, no panels.
function ColorSchemePicker() {
  const { preference, setPreference } = useColorScheme();
  return (
    <Tabs value={preference} onValueChange={(value) => setPreference(value as ColorSchemePreference)}>
      <TabsList aria-label="Appearance">
        {COLOR_SCHEME_OPTIONS.map(({ value, label, icon: Icon }) => (
          <TabsTrigger key={value} value={value}>
            <Icon />
            {label}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  );
}

export function SettingsDialog({ open, onOpenChange }: AppDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange} title="Settings" description="Manage your app preferences.">
      <FieldGroup>
        <Field orientation="responsive">
          <FieldContent>
            <FieldLabel>Appearance</FieldLabel>
            <FieldDescription>Choose how the app looks, or follow your system setting.</FieldDescription>
          </FieldContent>
          <ColorSchemePicker />
        </Field>
      </FieldGroup>
    </Dialog>
  );
}
