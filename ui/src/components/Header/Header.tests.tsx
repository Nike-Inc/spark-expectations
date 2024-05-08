import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { Header } from './Header';
import { useAuthStore } from '@/store';

// Mock the useAuthStore hook
vi.mock('@/store', () => ({
  useAuthStore: vi.fn(),
}));

describe('Header', () => {
  it('renders correctly with "Enter Token" when no token is present', () => {
    // @ts-ignore
    useAuthStore.mockImplementation(() => ({
      token: null,
      openModal: vi.fn(),
    }));

    render(<Header />);
    expect(screen.getByTestId('open-token-button')).toBeInTheDocument();
    expect(screen.getByText('Enter Token')).toBeInTheDocument();
  });

  it('renders correctly with "Update Token" when a token is present', () => {
    // @ts-ignore
    useAuthStore.mockImplementation(() => ({
      token: 'some-token',
      openModal: vi.fn(),
    }));

    render(<Header />);
    expect(screen.getByTestId('open-token-button')).toBeInTheDocument();
    expect(screen.getByText('Update Token')).toBeInTheDocument();
  });

  it('calls openModal when the button is clicked', () => {
    const mockOpenModal = vi.fn();
    // @ts-ignore
    useAuthStore.mockImplementation(() => ({
      token: null,
      openModal: mockOpenModal,
    }));

    render(<Header />);
    const button = screen.getByTestId('open-token-button');
    fireEvent.click(button);
    expect(mockOpenModal).toHaveBeenCalled();
  });

  // Additional tests can be added here
});
