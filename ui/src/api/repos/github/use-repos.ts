import { useQuery } from '@tanstack/react-query';
import { useAuthStore } from '@/store';
import { apiClient } from '@/api';
import { repoQueryKeys } from '../repo-query-keys';

export const getReposFn = async () => {
  const { username } = useAuthStore.getState();

  const response = await apiClient.get(`/users/${username || ''}/repos`, {
    headers: {
      Authorization: `Bearer ${useAuthStore.getState().token}`,
    },
  });

  return response.data as Repos[];
};

// TODO: How to convert Github User to generic Repo type?
export const useRepos = () =>
  useQuery({
    queryKey: repoQueryKeys.details(),
    queryFn: getReposFn,
    retry: 4,
  });
