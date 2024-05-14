import { render, screen } from '@testing-library/react';
import { AppProvider } from '@/providers';
import { useAuthStore } from '@/store';

const TestConsumer = () => {
  const { token, setToken } = useAuthStore();
  return (
    <div>
      <span>Token: {token}</span>
      <button type="button" onClick={() => setToken('test-token')}>
        Set Token
      </button>
    </div>
  );
};

describe('AuthProvider', () => {
  it('provides the auth context correctly', () => {
    render(
      <AppProvider>
        <TestConsumer />
      </AppProvider>
    );
    expect(screen.getByText('Token')).toBeInTheDocument();
  });
});
