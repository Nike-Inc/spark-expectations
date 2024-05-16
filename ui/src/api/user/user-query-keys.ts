export const userQueryKeys = {
  all: ['users'],
  details: () => [...userQueryKeys.all, 'detail'],
  detail: (id: string) => [...userQueryKeys.details(), id],
};
