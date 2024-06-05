export const repoQueryKeys = {
  all: ['repos'],
  detail: (params: any[]) => [...repoQueryKeys.all, ...params],
};
