import React from 'react';
import { ReactQueryProvider } from './react-query-provider';
import { CustomMantineProvider } from './mantine-provider';

export const AppProvider = ({ children }: { children: React.ReactNode }) => (
  <CustomMantineProvider>
    <ReactQueryProvider>{children}</ReactQueryProvider>
  </CustomMantineProvider>
);
