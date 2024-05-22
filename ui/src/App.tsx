import '@mantine/core/styles.css';
import '@mantine/notifications/styles.css';
import React, { FC } from 'react';
import './App.css';
import { AppProvider } from '@/providers';

export const App: FC = () => <AppProvider />;
