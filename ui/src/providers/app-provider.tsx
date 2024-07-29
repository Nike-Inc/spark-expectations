import React from 'react';
import { Notifications } from '@mantine/notifications';
import { ReactQueryProvider } from './react-query-provider';
import { CustomMantineProvider } from './mantine-provider';
import { RouterProvider } from './router-provider';

export const AppProvider = () => (
  <CustomMantineProvider>
    <ReactQueryProvider>
      <Notifications limit={5} position="top-right" autoClose={3000} />
      <RouterProvider />
    </ReactQueryProvider>
  </CustomMantineProvider>
);
