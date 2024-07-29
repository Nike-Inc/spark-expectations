import { MantineProvider } from '@mantine/core';
import React from 'react';
import { theme } from '@/theme';

export const CustomMantineProvider = ({ children }: { children: React.ReactNode }) => (
  <MantineProvider theme={theme}>{children}</MantineProvider>
);
