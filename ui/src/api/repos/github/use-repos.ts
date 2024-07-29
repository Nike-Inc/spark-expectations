import { useQuery } from '@tanstack/react-query';
import { useAuthStore } from '@/store';
import { apiClient } from '@/api';
import { repoQueryKeys } from '../repo-query-keys';

export const getReposFn = async () => {
  const response = await apiClient.get('/user/repos', {
    headers: {
      Authorization: `Bearer ${useAuthStore.getState().token}`,
    },
  });

  return response.data as Repo[];
};

export const useRepos = () =>
  useQuery({
    queryKey: repoQueryKeys.all,
    queryFn: getReposFn,
  });
