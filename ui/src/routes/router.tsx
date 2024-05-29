import { createBrowserRouter, createRoutesFromElements, Route } from 'react-router-dom';
import { Protected } from '@/routes/protected';
import { AppLayout } from '@/layouts';
import { Login } from '@/pages';
import { OAuthCallback } from '@/components';

export const router = createBrowserRouter(
  createRoutesFromElements(
    <Route path="/">
      <Route element={<Protected />}>
        <Route index element={<AppLayout />} />
      </Route>
      {/*<Route index element={<AppLayout />} />*/}
      <Route path="login" element={<Login />} />
      <Route path="/oauth/callback" element={<OAuthCallback />} />
      <Route path="*" element={<h1>Not Found</h1>} />
    </Route>
  )
);
