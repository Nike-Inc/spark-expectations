import '@mantine/core/styles.css';
import React, { FC } from 'react';
import './App.css';
// import { AppLayout } from '@/layouts';
import { AppProvider } from '@/providers';

export const App: FC = () => <AppProvider />;
