import { render as testingLibraryRender } from '@testing-library/react';
import React from 'react';
import { AppProvider } from '@/providers';

export function render(ui: React.ReactNode) {
  return testingLibraryRender(<>{ui}</>, {
    wrapper: ({ children }: { children: React.ReactNode }) => <AppProvider>{children}</AppProvider>,
  });
}
