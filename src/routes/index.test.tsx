import { screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { routeTree } from '@/routeTree';
import { renderWithRouter } from '@/test-utils';

describe('index route', () => {
  it('renders the home page', async () => {
    await renderWithRouter(routeTree, '/');
    expect(screen.getByRole('heading', { name: /kstack/i })).toBeInTheDocument();
  });
});
