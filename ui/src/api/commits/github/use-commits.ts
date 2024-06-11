import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api';
import { useRepoStore } from '@/store';

const fetchCommitsFn = async (): Promise<any[]> => {
  const { selectedRepo } = useRepoStore.getState();
  const branch: Branch | null = useRepoStore.getState().selectedBranch;
  const response = await apiClient.get(
    `/repos/${selectedRepo?.owner.login}/${selectedRepo?.name}/commits?sha=${branch?.commit.sha}`
  );
  return response.data as Commit[];
};

export const useCommits = (branchName: undefined | string, isEnabled = true) =>
  useQuery({
    queryFn: fetchCommitsFn,
    queryKey: ['Github', 'commits', branchName],
    enabled: isEnabled,
  });
