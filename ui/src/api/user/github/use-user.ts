import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api';
import { userQueryKeys } from '../user-query-keys';
import { useAuthStore } from '@/store';

export const getUserFn = async () => {
  const response = await apiClient.get('/user', {
    headers: {
      Authorization: `Bearer ${useAuthStore.getState().token}`,
    },
  });

  return response.data as User;
};

export const useUser = () =>
  useQuery({
    queryKey: userQueryKeys.detail('me'),
    queryFn: getUserFn,
  });
