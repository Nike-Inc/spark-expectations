import { render as testingLibraryRender } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import { createUserMock, useReposMock, useUserMock } from './__mocks__';
import { CustomMantineProvider, ReactQueryProvider } from '@/providers';

export function render(ui: React.ReactNode) {
  /*
   * Any updates to the store should be replicated here.
   * */
  vi.mock('@/store', () => {
    const useAuthStore = vi.fn(() => ({
      token: null,
      username: null,
      setToken: vi.fn(),
      setUserName: vi.fn(),
    }));

    return { useAuthStore };
  });

  /* If additional methods are added to api client, this wrapper needs to be updated
   * As api-client is an abstraction of the underlying GitHub client, extending the app to other git managers
   * will be easy. And this wrapper doesn't have to be updated.
   *  */

  vi.mock('@/api', () => {
    const getUserFn = vi.fn(() => Promise.resolve(createUserMock()));
    const useUser = vi.fn(() => useUserMock());
    // const getReposFn = vi.fn(() => Promise.resolve(Array.from({ length: 10 }, createRepoMock)));
    const useRepos = vi.fn(() => useReposMock());
    const useOAuth = vi.fn(() => ({
      data: { access_token: 'test' },
      isLoading: false,
      isError: false,
    }));

    const apiClient = {
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

    return { getUserFn, useUser, useRepos, apiClient, useOAuth };
  });

  return testingLibraryRender(<>{ui}</>, {
    wrapper: ({ children }: { children: React.ReactNode }) => (
      <MemoryRouter>
        <CustomMantineProvider>
          <ReactQueryProvider>{children}</ReactQueryProvider>
        </CustomMantineProvider>
      </MemoryRouter>
    ),
  });
}
