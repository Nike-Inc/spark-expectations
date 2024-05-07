import { useQuery } from '@tanstack/react-query';
import { gitApi } from '@/api';

const fetchUserInfo = async () => {
  const { data } = await gitApi.get('/user');
  return data;
};

export const useUserInfo = () =>
  useQuery({
    queryKey: ['user-info'],
    queryFn: fetchUserInfo,
    retry: 1,
  });
