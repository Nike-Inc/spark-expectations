import React, { FC } from 'react';
import { Container, Group } from '@mantine/core';

import { UserMenu } from './UserMenu';
import './Header.css';

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
