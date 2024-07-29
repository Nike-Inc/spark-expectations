import { Navigate, Outlet } from 'react-router-dom';
import { useEffect } from 'react';
import { useAuthStore } from '@/store';
import { useUser } from '@/api';
import { Loading } from '@/components';

export const Protected = () => {
  const { token, setUserName } = useAuthStore((state) => ({
    token: state.token,
    setUserName: state.setUserName,
  }));
  const { data, isLoading, isError } = useUser();

  useEffect(() => {
    if (isLoading) return;

    if (data && data.id) {
      setUserName(data.login);
    }
  }, [isLoading, data]);

  if (isLoading) {
    return <Loading />;
  }

  if (!token || isError) {
    return <Navigate to="/login" />;
  }

  return <Outlet />;
};
