import { useQuery } from '@tanstack/react-query';
import { gitApi } from '@/api';

const fetchArtifact = async () => {
  const { data } = await gitApi.get('/repos/cskcvarma/spark-expectations/contents/mkdocs.yml');
  return data;
};

export const useArtifact = () =>
  useQuery({
    queryKey: ['artifact'],
    queryFn: fetchArtifact,
    retry: 1,
  });
