import React, { FC } from 'react';
import { Container, Group } from '@mantine/core';

import { UserMenu } from './UserMenu';
import './Header.css';

// TODO: To be moved to layouts. Repos List and User Menu will be moved to root level of components
export const Header: FC = () => (
  <div className="header">
    <Container className="mainSection" size="md">
      <Group justify="space-between">
        <div>LOGO</div>
        <UserMenu />
      </Group>
    </Container>
  </div>
);
