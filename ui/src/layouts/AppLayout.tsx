import { AppShell, Button, Group, Skeleton } from '@mantine/core';
import React from 'react';
import { useAuthStore } from '@/store';

export const AppLayout = () => {
  const { openModal, token } = useAuthStore((state) => ({
    token: state.token,
    openModal: state.openModal,
  }));

  const tokenExists = !!token;

  return (
    <AppShell header={{ height: 60 }} navbar={{ width: 300, breakpoint: 'sm' }} padding="md">
      <AppShell.Header>
        <Group h="100%" px="md">
          <div>Logo</div>
          <Button name="open-token-button" data-testid="open-token-button" onClick={openModal}>
            {tokenExists ? 'Update Token' : 'Enter Token'}
          </Button>
        </Group>
      </AppShell.Header>
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
};
