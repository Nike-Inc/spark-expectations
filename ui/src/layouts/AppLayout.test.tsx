import { render, screen } from '@test-utils';
import { AppLayout } from './AppLayout';

describe('AppLayout', () => {
  it('renders navbar and main content', () => {
    render(<AppLayout />);
    expect(screen.getByTestId('navbar')).toBeInTheDocument();
    expect(screen.getByTestId('main-content')).toBeInTheDocument();
  });
});
