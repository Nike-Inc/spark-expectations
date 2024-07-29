import { createBrowserRouter, createRoutesFromElements, Route } from 'react-router-dom';
import { Protected } from './protected';
import { Login, OAuthCallback, Navigator } from '@/components';

export const router = createBrowserRouter(
  createRoutesFromElements(
    <Route path="/">
      <Route element={<Protected />}>
        <Route index element={<Navigator />} />
      </Route>
      {/*<Route index.ts element={<AppLayout />} />*/}
      <Route path="login" element={<Login />} />
      <Route path="/oauth/callback" element={<OAuthCallback />} />
      <Route path="*" element={<h1>Not Found</h1>} />
    </Route>
  )
);
