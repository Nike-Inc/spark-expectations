import React from 'react';
import { ReactQueryProvider } from './react-query-provider';
import { CustomMantineProvider } from './mantine-provider';
import { RouterProvider } from './router-provider';

export const AppProvider = () => (
  <CustomMantineProvider>
    <ReactQueryProvider>
      <RouterProvider />
    </ReactQueryProvider>
  </CustomMantineProvider>
);
