// tests for src/store/auth-store.ts
import { act } from 'react';
import { renderHook } from '@testing-library/react';
import { useAuthStore } from '@/store/auth-store';

describe('useAuthStore', () => {
  it('should set and update token', () => {
    const { result } = renderHook(() => useAuthStore());
    expect(result.current.token).toBeNull();

    act(() => {
      result.current.setToken('new-token');
    });
    expect(result.current.token).toBe('new-token');
  });
});
