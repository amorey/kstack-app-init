import { createRoute } from '@tanstack/react-router';

import { Route as rootRoute } from '@/routes/__root';

import '@/index.css';

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: Main,
});

function Main() {
  return (
    <main className="flex min-h-svh items-center justify-center p-6">
      <section className="w-full max-w-md rounded-xl border border-slate-200 bg-white/60 p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800/60">
        <h1 className="text-xl font-semibold text-slate-900 dark:text-slate-100">kstack</h1>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">Ready to build.</p>
      </section>
    </main>
  );
}
