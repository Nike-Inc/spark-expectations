import React, { FC } from 'react';
import { Group } from '@mantine/core';

import { UserMenu } from '@/components';
import './Header.css';

export const Header: FC = () => (
  <Group justify="space-between">
    <div>LOGO</div>
    <UserMenu />
  </Group>
);
