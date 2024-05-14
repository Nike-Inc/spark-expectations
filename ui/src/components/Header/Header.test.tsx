import { describe, it, expect } from 'vitest';
import { render, screen } from '@test-utils';
import { Header } from './Header';

describe('Header', () => {
  it('renders without crashing', () => {
    render(<Header />);
    expect(screen.getByText('LOGO')).toBeInTheDocument();
  });
});
