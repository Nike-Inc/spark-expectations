import { render as testingLibraryRender } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';
import { AppProvider } from '@/providers';
import { createDynamicMockStore } from './__mocks__/dynamic-store.mock';
import { createUserMock, useReposMock, useUserMock } from './__mocks__';

export function render(ui: React.ReactNode) {
  /*
   * Any updates to the store should be replicated here.
   * */
  vi.mock('@/store', () => {
    const useAuthStore = () =>
      createDynamicMockStore({
        token: 'test-token',
        isModalOpen: false,
        username: 'test-username',
      });
    return { useAuthStore };
  });

  /* If additional methods are added to api client, this wrapper needs to be updated
   * As api-client is an abstraction of the underlying GitHub client, extending the app to other git managers
   * will be easy. And this wrapper doesn't have to be updated.
   *  */
  vi.mock('@/api/api-client', () => {
    const mockAxiosInstance = {
      get: vi.fn(() => Promise.resolve({ data: 'mocked get' })),
      post: vi.fn(() => Promise.resolve({ data: 'mocked post' })),
      put: vi.fn(() => Promise.resolve({ data: 'mocked put' })),
      delete: vi.fn(() => Promise.resolve({ data: 'mocked delete' })),
      patch: vi.fn(() => Promise.resolve({ data: 'mocked patch' })),
      head: vi.fn(() => Promise.resolve({ data: 'mocked head' })),
      options: vi.fn(() => Promise.resolve({ data: 'mocked options' })),
      request: vi.fn(() => Promise.resolve({ data: 'mocked request' })),
      interceptors: {
        request: { use: vi.fn(), eject: vi.fn() },
        response: { use: vi.fn(), eject: vi.fn() },
      },
      defaults: { headers: { common: {} } },
    };
    return {
      apiClient: mockAxiosInstance,
    };
  });

  vi.mock('@/api/user', () => {
    const getUserFn = vi.fn(() => Promise.resolve(createUserMock()));
    const useUser = vi.fn(() => useUserMock());
    return { getUserFn, useUser };
  });

  vi.mock('@/api/repo', () => {
    const useRepos = vi.fn(() => useReposMock());
    return { useRepos };
  });

  return testingLibraryRender(<>{ui}</>, {
    wrapper: ({ children }: { children: React.ReactNode }) => <AppProvider>{children}</AppProvider>,
  });
}
