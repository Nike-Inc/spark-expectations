import yaml from 'js-yaml';
import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api';
import { useAuthStore } from '@/store';
import { repoQueryKeys } from '@/api/repos/repo-query-keys';

export const readYamlFile = async (owner: any, repoName: any, path: any) => {
  const response = await apiClient.get(`repos/${owner}/${repoName}/contents/${path}`, {
    headers: {
      Authorization: `Bearer ${useAuthStore.getState().token}`,
    },
    // params: {
    //   ref: 'rule_changer',
    // },
  });
  const { content, encoding } = response.data;

  if (encoding !== 'base64') {
    throw new Error('Unsupported encoding');
  }

  const decodedContent = atob(content);
  return yaml.load(decodedContent) as unknown as Record<string, any>;
};

export const useRepoFile = (
  owner: string | undefined,
  repoName: string | undefined,
  path: string | undefined,
  isEnabled = true
) =>
  useQuery({
    queryKey: repoQueryKeys.detail(['file', owner, repoName, path]),
    queryFn: () => readYamlFile(owner, repoName, path),
    enabled: isEnabled && !!owner && !!repoName && !!path,
  });
