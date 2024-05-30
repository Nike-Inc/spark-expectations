import React from 'react';
import { render, screen, waitFor, userEvent } from '@test-utils';
import { vi } from 'vitest';
import { Login } from '@/components';
import { githubLogin } from '@/api';

describe('Login Component', () => {
  beforeEach(() => {
    // Clear all mocks before each test
    vi.clearAllMocks();
  });

  it('renders correctly', () => {
    render(<Login />);
    expect(screen.getByText('Spark Expectations')).toBeInTheDocument();
    expect(
      screen.getByText('Please login using one of the following providers:')
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Login with GitHub' })).toBeInTheDocument();
  });

  it('calls githubLogin on button click', async () => {
    render(<Login />);
    const loginButton = screen.getByRole('button', { name: 'Login with GitHub' });
    userEvent.click(loginButton);
    await waitFor(() => expect(githubLogin).toHaveBeenCalled());
  });

  it('handles the login process correctly', async () => {
    // Assume githubLogin is a promise that resolves to an access token
    (githubLogin as jest.Mock).mockResolvedValue({ access_token: 'fake-token' });

    render(<Login />);
    const loginButton = screen.getByRole('button', { name: 'Login with GitHub' });
    userEvent.click(loginButton);

    await waitFor(() => {
      // You can add additional assertions here to check for changes in the UI or redirects
      expect(githubLogin).toHaveBeenCalled();
    });
  });
});
