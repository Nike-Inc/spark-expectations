import { describe, it, expect, vi } from 'vitest';
import { useAuthStore } from '@/store';
import { isAuthenticated } from '@/utils/isAuthenticated';

// Mock the useAuthStore hook
vi.mock('@/store', () => ({
  useAuthStore: vi.fn(),
}));

describe('isAuthenticated', () => {
  it('returns false when the token is missing', () => {
    // Set up the mock to return no token
    // @ts-ignore
    useAuthStore.mockImplementation(() => ({
      token: null,
      username: 'some-username',
    }));

    // Call the function and check the result
    expect(isAuthenticated()).toBe(false);
  });

  it('returns false when the username is missing', () => {
    // Set up the mock to return no username
    // @ts-ignore
    useAuthStore.mockImplementation(() => ({
      token: 'some-token',
      username: null,
    }));

    // Call the function and check the result
    expect(isAuthenticated()).toBe(false);
  });

  it('returns false when both token and username are missing', () => {
    // Set up the mock to return neither token nor username
    // @ts-ignore
    useAuthStore.mockImplementation(() => ({
      token: null,
      username: null,
    }));

    // Call the function and check the result
    expect(isAuthenticated()).toBe(false);
  });
});
