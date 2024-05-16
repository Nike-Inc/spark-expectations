import React, { ReactNode, useEffect } from 'react';
import { Modal, TextInput, Button, Group } from '@mantine/core';
import { useForm } from '@mantine/form';
import { useAuthStore } from '@/store';
import { getUserFn } from '@/api';
import { Loading } from '@/components/Loading/Loading';

interface AuthProviderProps {
  children: ReactNode;
}

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const { token, username, isModalOpen, setUserName, setToken, openModal, closeModal } =
    useAuthStore();

  const form = useForm({
    initialValues: {
      token,
    },
    validate: {
      token: (value: string | null) => value && value.length === 0 && 'Token is required',
    },
  });

  useEffect(() => {
    if (!token) {
      openModal();
    } else {
      closeModal();
    }
  }, [token, openModal, closeModal]);

  const handleLogin = (values: { token: null | string }) => {
    setToken(values?.token);

    getUserFn().then((res) => {
      setUserName(res.login);
    });

    closeModal();
  };

  const handleCancel = () => {
    if (token) closeModal();
  };

  return (
    <>
      <Modal
        opened={isModalOpen}
        onClose={closeModal}
        title="Enter Your Token"
        closeOnClickOutside={false}
        withCloseButton={false}
      >
        <form onSubmit={form.onSubmit((values) => handleLogin(values))}>
          <TextInput {...form.getInputProps('token')} label="Token" placeholder="Token" required />
          <Group mt="md">
            <Button type="button" onClick={handleCancel} color="red">
              Cancel
            </Button>
            <Button type="submit">Submit</Button>
          </Group>
        </form>
      </Modal>
      {/*TODO: Handle Error State*/}
      {token && username ? children : <Loading />}
    </>
  );
};
