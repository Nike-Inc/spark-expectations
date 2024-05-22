import { useAuthStore } from '@/store/auth-store';

/**
 * Checks if the user is authenticated based on the presence of a token and a username.
 * @returns {boolean} True if the user is authenticated, false otherwise.
 */
export const isAuthenticated = (): boolean => {
  const { token, username } = useAuthStore.getState();
  return Boolean(token && username);
};
