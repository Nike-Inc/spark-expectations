import { AppShell, Button, Group, Skeleton } from '@mantine/core';
import React from 'react';
import { useAuthStore } from '@/store';
import { Header } from '@/components';

export const AppLayout = () => (
  <AppShell header={{ height: 60 }} navbar={{ width: 300, breakpoint: 'sm' }} padding="md">
    {/*TODO: data-testid is not retained after the component is mounted. Need to investigate this*/}
    <Header data-testid="header" />
    <AppShell.Navbar p="md" data-testid="navbar">
      Navbar
      {Array(15)
        .fill(0)
        .map((_, index) => (
          <Skeleton key={index} h={28} mt="sm" animate={false} />
        ))}
    </AppShell.Navbar>
    <AppShell.Main data-testid="main-content">Main</AppShell.Main>
  </AppShell>
);
