import { Navigate, Outlet } from 'react-router-dom';
import { useAuthStore } from '@/store';

export const Protected = () => {
  const { token, username } = useAuthStore((state) => ({
    token: state.token,
    username: state.username,
  }));

  if (!token || !username) {
    return <Navigate to="/login" />;
  }

  return <Outlet />;
};
