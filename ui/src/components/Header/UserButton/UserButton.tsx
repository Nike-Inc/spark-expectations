import { Avatar, Group, Text, UnstyledButton, rem, Skeleton, Alert } from '@mantine/core';
import { IconAlertCircle, IconChevronRight } from '@tabler/icons-react';
import './UserButton.css';
import { useUser } from '@/api';
import { useAuthStore } from '@/store';

// TODO: Handle Token Check in a different place
export const UserButton = () => {
  const { token } = useAuthStore();

  if (!token) {
    return <LoadingUserButton />;
  }

  return <UserButtonWithToken />;
};

const UserButtonWithToken = () => {
  const { data, error, isLoading } = useUser();

  if (isLoading) {
    return <LoadingUserButton />;
  }

  if (error) {
    return <ErrorUserButton />;
  }

  return (
    <UnstyledButton>
      <Group>
        <Avatar src={data?.avatar_url} radius="xl" />

        <div style={{ flex: 1 }}>
          <Text size="sm" fw={500}>
            {data?.login}
          </Text>

          <Text c="dimmed" size="xs">
            {data?.email}
          </Text>
        </div>
        <IconChevronRight className="IconChevronRight" />
      </Group>
    </UnstyledButton>
  );
};

const ErrorUserButton = () => (
  <UnstyledButton>
    <Group>
      <div style={{ flex: 1 }}>
        <Alert icon={<IconAlertCircle size={16} />} color="red">
          Failed to load user data
        </Alert>
      </div>
    </Group>
  </UnstyledButton>
);

const LoadingUserButton = () => (
  <UnstyledButton>
    <Group>
      <Skeleton circle width={40} height={40} />
      <div style={{ flex: 1 }}>
        <Skeleton height={8} width="50%" mb="xs" />
        <Skeleton height={8} width="30%" />
      </div>
      <Skeleton width={20} height={20} />
    </Group>
  </UnstyledButton>
);
