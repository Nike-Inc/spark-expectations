import { createRepoMock } from './repo.mock';

export const useReposMock = () => ({
  data: Array.from({ length: 10 }, createRepoMock),
  isLoading: false,
  isError: false,
});
