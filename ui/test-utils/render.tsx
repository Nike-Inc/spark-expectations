import { render as testingLibraryRender } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';
import { AppProvider } from '@/providers';

export function render(ui: React.ReactNode) {
  vi.mock('@/store', () => ({
    useAuthStore: vi.fn(() => ({
      token: 'mock-token',
      username: 'mock-username',
      openModal: vi.fn(),
      closeModal: vi.fn(),
    })),
  }));

  vi.mock('@/api/github-client', () => ({
    gitHubClient: vi.fn(() => ({
      get: vi.fn(() => Promise.resolve({ data: 'mocked data' })),
      post: vi.fn(() => Promise.resolve({ data: 'mocked response' })),
      // Add other methods as needed
    })),
  }));

  return testingLibraryRender(<>{ui}</>, {
    wrapper: ({ children }: { children: React.ReactNode }) => <AppProvider>{children}</AppProvider>,
  });
}
