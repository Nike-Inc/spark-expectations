import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api';
import { useAuthStore, useRepoStore } from '@/store';

export const getBranchFn = async () => {
  const { selectedRepo } = useRepoStore.getState();

  const response = await apiClient.get(
    `/repos/${selectedRepo?.owner.login}/${selectedRepo?.name}/branches/rule_changer`,
    {
      headers: {
        Authorization: `Bearer ${useAuthStore.getState().token}`,
      },
    }
  );
  return response.data;
};

export const useBranch = () =>
  useQuery({
    queryKey: ['Github', 'branch'],
    queryFn: getBranchFn,
  });
