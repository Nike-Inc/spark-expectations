import React from 'react';
import { ReactQueryProvider } from './react-query-provider';
import { CustomMantineProvider } from './mantine-provider';
import { AuthProvider } from './auth-provider';

export const AppProvider = ({ children }: { children: React.ReactNode }) => (
  <CustomMantineProvider>
    <ReactQueryProvider>
      <AuthProvider>{children}</AuthProvider>
    </ReactQueryProvider>
  </CustomMantineProvider>
);
