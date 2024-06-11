import React, { FC } from 'react';
import { Group, Image } from '@mantine/core';

import { UserMenu } from '@/components';
import './Header.css';

export const Header: FC = () => (
  <Group justify="space-between">
    <Image src="/assets/logo.png" width={200} height={50} ml={18} />
    <UserMenu />
  </Group>
);
