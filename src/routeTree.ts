import { Route as rootRoute } from '@/routes/__root';
import { Route as indexRoute } from '@/routes/index';

export const routeTree = rootRoute.addChildren([indexRoute]);
