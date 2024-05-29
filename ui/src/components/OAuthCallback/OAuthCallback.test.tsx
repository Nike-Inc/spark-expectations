import { describe } from 'vitest';
import { render, screen } from '@test-utils';
import { waitFor } from '@testing-library/react';
import { useNavigate } from 'react-router-dom';
import { useOAuth } from '@/api';
import { OAuthCallback } from '@/components';
import { useAuthStore } from '@/store';

describe('OAuthCallback', () => {
  test('displays loading message while fetching data', () => {
    //@ts-ignore
    useOAuth.mockReturnValue({ data: null, isLoading: true, isError: false });
    render(<OAuthCallback />);
    expect(screen.getByText('Loading...')).toBeInTheDocument();
  });

  test('sets token and navigates upon receving data', async () => {
    const mockSetToken = vi.fn();
    const mockNavigate = vi.fn();
    //@ts-ignore
    useOAuth.mockReturnValue({
      data: { access_token: 'token' },
      isLoading: false,
      isError: false,
    });
    // @ts-ignore
    useAuthStore.mockReturnValue({ setToken: mockSetToken });
    // @ts-ignore
    useNavigate.mockReturnValue(mockNavigate);

    render(<OAuthCallback />);
    await waitFor(
      () => {
        expect(mockSetToken).toHaveBeenCalledWith('token');
        expect(mockNavigate).toHaveBeenCalledWith('/');
      },
      {
        timeout: 1000,
      }
    );
  });

  test('displays error message if error occurs', () => {
    const mockNavigate = vi.fn();
    //@ts-ignore
    useOAuth.mockReturnValue({
      data: null,
      isLoading: false,
      isError: true,
      error: { message: 'error' },
    });
    //@ts-ignore
    useNavigate.mockReturnValue(mockNavigate);
    render(<OAuthCallback />);
    expect(mockNavigate).toHaveBeenCalledWith('/login');
  });
});
