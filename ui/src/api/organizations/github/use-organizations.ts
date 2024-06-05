import { useQuery } from '@tanstack/react-query';
import { apiClient } from '@/api';
import { useAuthStore } from '@/store';
import { organizationQueryKeys } from '../query-keys';

export const getOrganizationsFn = async () => {
  const response = await apiClient.get('/user/repos', {
    headers: {
      Authorization: `Bearer ${useAuthStore.getState().token}`,
    },
  });

  return response.data;
};

export const useOrganizations = () =>
  useQuery({
    queryKey: organizationQueryKeys.all,
    queryFn: getOrganizationsFn,
  });
