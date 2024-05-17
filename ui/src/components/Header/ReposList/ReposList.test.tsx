import { describe, it, expect } from 'vitest';
import { render, screen } from '@test-utils';
import { ReposList } from './ReposList';
import { useRepos } from '@/api';

describe('ReposList', () => {
  it('renders loading state correctly', () => {
    // @ts-ignore
    useRepos.mockReturnValue({ data: null, error: null, isLoading: true });
    render(<ReposList />);
    expect(screen.getByTestId('loading-user-menu')).toBeInTheDocument();
  });

  it('renders error state correctly', () => {
    // @ts-ignore
    useRepos.mockReturnValue({ data: null, error: 'Failed to fetch', isLoading: false });
    render(<ReposList />);
    expect(screen.getByText('Error!')).toBeInTheDocument();
  });

  it('renders repository list correctly', () => {
    const repos = [{ name: 'Repo1' }, { name: 'Repo2' }, { name: 'Repo3' }];
    // @ts-ignore
    useRepos.mockReturnValue({ data: repos, error: null, isLoading: false });
    render(<ReposList />);
    expect(screen.getByText('Repo1')).toBeInTheDocument();
    expect(screen.getByText('Repo2')).toBeInTheDocument();
    expect(screen.getByText('Repo3')).toBeInTheDocument();
  });
});
