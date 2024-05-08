import React, { FC } from 'react';
import { AppShellHeader, Button, Group } from '@mantine/core';
import { useAuthStore } from '@/store';
import { ReposList } from './ReposList';

export const Header: FC = () => {
  const { openModal, token } = useAuthStore((state) => ({
    token: state.token,
    openModal: state.openModal,
  }));

  const tokenExists = !!token;

  return (
    <AppShellHeader data-testid="header">
      <Group h="100%" px="md" justify="space-between">
        <div>Logo</div>

        <ReposList />

        <div>
          <h4> Spark Expectations</h4>
        </div>

        <Button name="open-token-button" data-testid="open-token-button" onClick={openModal}>
          {tokenExists ? 'Update Token' : 'Enter Token'}
        </Button>
      </Group>
    </AppShellHeader>
  );
};
