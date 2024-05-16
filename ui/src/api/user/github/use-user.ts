import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api';
import { userQueryKeys } from '../user-query-keys';

export const getUserFn = async () => {
  const response = await apiClient.get('/user');

  return response.data as User;
};

// TODO: How to convert Github User to generic User type?
export const useUser = () =>
  useQuery({
    queryKey: userQueryKeys.detail('me'),
    queryFn: getUserFn,
    retry: 4,
  });
