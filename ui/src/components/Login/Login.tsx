import { FC } from 'react';
import { Button, Container, Paper, Text, Title } from '@mantine/core';

import { githubLogin } from '@/api';

export const Login: FC = () => {
  const loginWithGithub = () => {
    githubLogin();
  };
  return (
    <>
      <Container size={420} my={40}>
        <Title ta="center">Spark Expectations</Title>
        <Text c="dimmed" size="sm" ta="center" mt={5}>
          Please login using one of the following providers:
        </Text>

        <Paper withBorder shadow="md" p={30} mt={30} radius="md">
          <Button fullWidth color="blue" onClick={() => loginWithGithub()}>
            Login with GitHub
          </Button>
        </Paper>
      </Container>
    </>
  );
};
