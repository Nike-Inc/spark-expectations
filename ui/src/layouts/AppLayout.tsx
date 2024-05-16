import { AppShell } from '@mantine/core';
import React from 'react';

import { Header } from '@/components';
import { NavBar } from '@/components/NavBar';

export const AppLayout = () => (
  <AppShell header={{ height: 60 }} navbar={{ width: 300, breakpoint: 'sm' }} padding="md">
    {/*TODO: data-testid is not retained after the component is mounted. Need to investigate this*/}
    <Header aria-label="header" />
    <AppShell.Navbar p="md" aria-label="navbar">
      <NavBar />
    </AppShell.Navbar>

    {/*<AppShell.Navbar p="md" data-testid="navbar">*/}
    {/*  Navbar*/}
    {/*  {Array(15)*/}
    {/*    .fill(0)*/}
    {/*    .map((_, index) => (*/}
    {/*      <Skeleton key={index} h={28} mt="sm" animate={false} />*/}
    {/*    ))}*/}
    {/*</AppShell.Navbar>*/}
    <AppShell.Main aria-label="main-content">Main</AppShell.Main>
  </AppShell>
);
