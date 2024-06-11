import { Buffer } from 'buffer';
import { useAuthStore, useRepoStore } from '@/store';
import { apiClient } from '@/api';

// @ts-ignore
export const commitChangesFn = async ({ content }) => {
  const { selectedRepo, selectedBranch, selectedFile } = useRepoStore.getState();

  await apiClient.put(
    `/repos/${selectedRepo?.owner.login}/${selectedRepo?.name}/contents/${selectedFile.path}`,
    {
      message: 'Update rule_changer.yaml',
      content: Buffer.from(content).toString('base64'),
      sha: selectedFile.sha,
      branch: selectedBranch?.name,
    },
    {
      headers: {
        Authorization: `Bearer ${useAuthStore.getState().token}`,
      },
    }
  );
};
