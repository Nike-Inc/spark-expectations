export const repoQueryKeys = {
  all: ['repos'],
  details: () => [...repoQueryKeys.all, 'details'],
  detail: (id: string) => [...repoQueryKeys.details(), id],
};
