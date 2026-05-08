import { useState } from 'react';
import { createRoute } from '@tanstack/react-router';
import { invoke } from '@tauri-apps/api/core';

import reactLogo from '@/assets/react.svg';
import { Route as rootRoute } from '@/routes/__root';

import '@/App.css';

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: Main,
});

function Main() {
  const [greetMsg, setGreetMsg] = useState('');
  const [name, setName] = useState('');

  async function greet() {
    setGreetMsg(await invoke('greet', { name }));
  }

  return (
    <main className="container">
      <h1>Welcome to Tauri + React</h1>

      <div className="row">
        <a href="https://vite.dev" target="_blank">
          <img src="/vite.svg" className="logo vite" alt="Vite logo" />
        </a>
        <a href="https://tauri.app" target="_blank">
          <img src="/tauri.svg" className="logo tauri" alt="Tauri logo" />
        </a>
        <a href="https://react.dev" target="_blank">
          <img src={reactLogo} className="logo react" alt="React logo" />
        </a>
      </div>
      <p>Click on the Tauri, Vite, and React logos to learn more.</p>

      <form
        className="row"
        onSubmit={(e) => {
          e.preventDefault();
          greet();
        }}
      >
        <input id="greet-input" onChange={(e) => setName(e.currentTarget.value)} placeholder="Enter a name..." />
        <button type="submit">Greet</button>
      </form>
      <p>{greetMsg}</p>

      <section className="mt-12 mx-auto max-w-md rounded-xl border border-slate-200 bg-white/60 p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800/60">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Tailwind v4 is wired up</h2>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
          This card is styled entirely with utility classes — no custom CSS.
        </p>
      </section>
    </main>
  );
}
