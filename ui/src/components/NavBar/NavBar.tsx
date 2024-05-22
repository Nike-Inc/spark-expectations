import './NavBar.css';
import { ReposList } from '@/components/Header/ReposList';

// TODO: To be moved to Layouts
export const NavBar = () => (
  <nav>
    <div>
      <ReposList />
    </div>
  </nav>
);
