import { Navigate, Outlet } from 'react-router-dom';
import { useAuthStore } from '@/store';

export const Protected = () => {
  const { token } = useAuthStore.getState();

  if (!token) {
    return <Navigate to="/login" />;
  }

  return <Outlet />;
};
