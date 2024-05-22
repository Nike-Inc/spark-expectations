import { createBrowserRouter, createRoutesFromElements, Route } from 'react-router-dom';
import { Protected } from '@/routes/protected';
import { AppLayout } from '@/layouts';
import { LoginPage } from '@/pages';
import { isAuthenticated } from '@/utils';

export const router = createBrowserRouter(
  createRoutesFromElements(
    <Route path="/">
      <Route element={<Protected />}>
        <Route index element={<AppLayout />} />
      </Route>

      <Route path="login" element={<LoginPage />} loader={async () => isAuthenticated()} />
      <Route path="*" element={<h1>Not Found</h1>} />
    </Route>
  )
);
