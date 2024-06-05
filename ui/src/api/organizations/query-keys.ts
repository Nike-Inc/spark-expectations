export const organizationQueryKeys = {
  all: ['organizations'],
  details: () => [...organizationQueryKeys.all, 'detail'],
  detail: (id: string) => [...organizationQueryKeys.details(), id],
};
