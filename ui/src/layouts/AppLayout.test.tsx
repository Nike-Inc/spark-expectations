import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@test-utils';
import { AppLayout } from './AppLayout';
import { useAuthStore } from '@/store/auth-store';

vi.mock('@/store/auth-store', () => ({
  useAuthStore: vi.fn(() => ({
    token: null,
    openModal: vi.fn(),
    closeModal: vi.fn(),
  })),
}));

describe('AppLayout', () => {
  it('renders navbar and main content', () => {
    render(<AppLayout />);
    expect(screen.getByTestId('navbar')).toBeInTheDocument();
    expect(screen.getByTestId('header')).toBeInTheDocument();
    expect(screen.getByTestId('main-content')).toBeInTheDocument();
  });
});
