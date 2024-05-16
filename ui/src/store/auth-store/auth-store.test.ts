// tests for src/store/auth-store.ts
import { act } from 'react-dom/test-utils';
import { renderHook } from '@testing-library/react';
import { useAuthStore } from '@/store/auth-store';

describe('useAuthStore', () => {
  it('should toggle modal open and close', () => {
    const { result } = renderHook(() => useAuthStore());
    expect(result.current.isModalOpen).toBe(false);

    act(() => {
      result.current.openModal();
    });
    expect(result.current.isModalOpen).toBe(true);

    act(() => {
      result.current.closeModal();
    });
    expect(result.current.isModalOpen).toBe(false);
  });

  it('should set and update token', () => {
    const { result } = renderHook(() => useAuthStore());
    expect(result.current.token).toBeNull();

    act(() => {
      result.current.setToken('new-token');
    });
    expect(result.current.token).toBe('new-token');
  });
});
