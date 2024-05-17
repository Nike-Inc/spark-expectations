//
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@test-utils';
import { UserMenu } from './UserMenu';
import { useUser } from '@/api';

describe('UserMenu', () => {
  it('renders loading state correctly', () => {
    // @ts-ignore
    useUser.mockReturnValue({ data: null, error: null, isLoading: true });
    render(<UserMenu />);
    expect(screen.getByTestId('loading-user-menu')).toBeInTheDocument();
  });

  it('renders error state correctly', () => {
    // @ts-ignore
    useUser.mockReturnValue({ data: null, error: 'Failed to load user data', isLoading: false });
    render(<UserMenu />);
    expect(screen.getByText('Failed to load user data')).toBeInTheDocument();
  });

  it('renders user information correctly', () => {
    const userData = {
      name: 'John Doe',
      avatar_url: 'http://example.com/avatar.jpg',
    };
    // @ts-ignore
    useUser.mockReturnValue({ data: userData, error: null, isLoading: false });
    render(<UserMenu />);
    expect(screen.getByText('John Doe')).toBeInTheDocument();
    expect(screen.getByRole('img', { name: 'John Doe' })).toHaveAttribute(
      'src',
      'http://example.com/avatar.jpg'
    );
  });
});
