import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api';
import { useAuthStore } from '@/store';
import { repoQueryKeys } from '@/api/repos/repo-query-keys';

export const getRepoFn = async (
  owner: string | undefined,
  repoName: string | undefined,
  path = ''
) => {
  const response = await apiClient.get(`repos/${owner}/${repoName}/contents/${path}`, {
    headers: {
      Authorization: `Bearer ${useAuthStore.getState().token}`,
    },
  });

  return response.data;
};

export const getAllYamlFilesFn = async (
  owner: string | undefined,
  repoName: string | undefined,
  initialPath = ''
) => {
  async function fetchDirectory(path: any) {
    try {
      const response = await apiClient.get(`repos/${owner}/${repoName}/contents/${path}`, {
        headers: {
          Authorization: `Bearer ${useAuthStore.getState().token}`,
        },
      });
      return response.data;
    } catch (error) {
      console.log(error);
      throw error;
    }
  }

  const fetchRecursive = async (path: string) => {
    const items = await fetchDirectory(path);
    const files = items.filter(
      (item: { type: string; name: string }) => item.type === 'file' && item.name.endsWith('.yaml')
    );
    const directories = items.filter((item: { type: string }) => item.type === 'dir');

    const directoryPromises = directories.map((directory: { path: string }) =>
      fetchRecursive(directory.path)
    );
    const results = await Promise.all(directoryPromises);
    return files.concat(results.flat());
  };

  return fetchRecursive(initialPath);
};

export const useRepo = (
  owner: string | undefined,
  repoName: string | undefined,
  path = '',
  isEnabled = true
) =>
  useQuery({
    queryKey: repoQueryKeys.detail([owner, repoName, path]),
    queryFn: () => getAllYamlFilesFn(owner, repoName, path),
    enabled: isEnabled && !!owner && !!repoName,
  });

// function filterYAMLFiles(data: any) {
//   return data.filter(
//     (item: any) => item.type === 'dir' || (item.type === 'file' && item.name.endsWith('.yaml'))
//   );
// }
