import { render, screen } from '@test-utils';
import { App } from './App';

describe('App', () => {
  it('renders main content without crashing', () => {
    render(<App />);
    expect(screen.getByTestId('main-content')).toBeInTheDocument();
  });
  it('renders navbar without crashing', () => {
    render(<App />);
    expect(screen.getByTestId('navbar')).toBeInTheDocument();
  });
});
