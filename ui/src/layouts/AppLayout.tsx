import { AppShell } from '@mantine/core';
import React from 'react';

import { NavBar } from './NavBar';
import { Header } from './Header';

export const AppLayout = () => (
  <AppShell header={{ height: 60 }} navbar={{ width: 300, breakpoint: 'sm' }} padding="md">
    <Header aria-label="header" />
    <AppShell.Navbar p="md" aria-label="navbar">
      <NavBar />
    </AppShell.Navbar>
    <AppShell.Main aria-label="main-content">Main</AppShell.Main>
  </AppShell>
);
