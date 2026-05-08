import { act, render } from '@testing-library/react';
import { createMemoryHistory, createRouter, RouterProvider } from '@tanstack/react-router';
import type { AnyRoute } from '@tanstack/react-router';

export async function renderWithRouter(routeTree: AnyRoute, path: string) {
  const router = createRouter({
    routeTree,
    history: createMemoryHistory({ initialEntries: [path] }),
  });

  await router.load();

  let result!: ReturnType<typeof render>;
  await act(async () => {
    result = render(<RouterProvider router={router} />);
  });

  return {
    ...result,
    router,
  };
}
