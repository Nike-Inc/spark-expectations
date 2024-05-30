import React from 'react';

import { AppShell, Container, Grid, Skeleton } from '@mantine/core';
import { Header } from '@/components';

const child = <Skeleton height="900" radius="md" animate={false} />;

export const Navigator = () => (
  <AppShell>
    <AppShell.Header>
      <Header />
    </AppShell.Header>
    <AppShell.Main>
      <Container fluid mt={50}>
        <Grid>
          <Grid.Col span={3}>{child}</Grid.Col>
          <Grid.Col span={6}>{child}</Grid.Col>
          <Grid.Col span={3}>{child}</Grid.Col>
        </Grid>
      </Container>
    </AppShell.Main>
  </AppShell>
);
