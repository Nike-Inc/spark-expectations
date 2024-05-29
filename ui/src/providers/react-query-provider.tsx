import { PropsWithChildren } from 'react';
import { QueryCache, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ReactQueryDevtools } from '@tanstack/react-query-devtools';
import { notifications } from '@mantine/notifications';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
    },
  },
  queryCache: new QueryCache({
    onError: (error, query) =>
      notifications.show({
        title: `${error.name} for ${query.queryKey}`,
        message: error.message,
        color: 'red',
      }),
  }),
});

export const ReactQueryProvider = ({ children }: PropsWithChildren) => (
  <QueryClientProvider client={queryClient}>
    {children}
    <ReactQueryDevtools initialIsOpen={false} />
  </QueryClientProvider>
);
