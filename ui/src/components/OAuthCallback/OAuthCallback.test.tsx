import { describe } from 'vitest';
import { render, screen } from '@test-utils';
import { useOAuth } from '@/api';
import { OAuthCallback } from '@/components';

describe('OAuthCallback', () => {
  it('displays loading message while fetching data', () => {
    //@ts-ignore
    useOAuth.mockReturnValue({ data: null, isLoading: true, isError: false });
    render(<OAuthCallback />);
    expect(screen.getByText('Loading...')).toBeInTheDocument();
  });
});
