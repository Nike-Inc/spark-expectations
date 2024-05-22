import React, { FC } from 'react';
import { useForm } from '@mantine/form';
import { TextInput, Button, Group, Container, Title } from '@mantine/core';
import { useNavigate } from 'react-router-dom';
import { showNotification } from '@mantine/notifications';
import { useAuthStore } from '@/store';
import { getUserFn } from '@/api';
import './Login.css';

interface LoginPageProps {}

export const LoginPage: FC<LoginPageProps> = () => {
  const { setToken, setUserName } = useAuthStore();
  const navigate = useNavigate();
  const form = useForm({
    initialValues: {
      token: '',
    },
    validate: {
      token: (value: string) => (value ? null : 'Token is required'),
    },
  });

  // @ts-ignore
  const handleLogin = async ({ token }) => {
    setToken(token);
    getUserFn()
      .then((res) => {
        setUserName(res.login);
        navigate('/');
      })
      .catch(() => {
        showNotification({
          title: 'Error while logging in',
          message: 'Please check your token and try again',
          color: 'red',
          withCloseButton: true,
        });
      });
  };

  return (
    <Container className="login-page">
      <Title className="login-title">Login</Title>
      <form onSubmit={form.onSubmit(handleLogin)}>
        <TextInput
          {...form.getInputProps('token')}
          label="Token"
          placeholder="Enter your token"
          required
          name="token-input"
        />
        <Group mt="md">
          <Button type="submit">Login</Button>
        </Group>
      </form>
    </Container>
  );
};
