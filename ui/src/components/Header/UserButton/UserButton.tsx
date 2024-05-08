import { Avatar, Group, Text, UnstyledButton, rem } from '@mantine/core';
import { IconChevronRight } from '@tabler/icons-react';
import './UserButton.css';

// TODO: Integrate this with API, implement a loading state, error handling, and hover state
export const UserButton = () => {
  console.log('tbd');

  // @ts-ignore
  return (
    <UnstyledButton>
      <Group>
        <Avatar
          src="https://raw.githubusercontent.com/mantinedev/mantine/master/.demo/avatars/avatar-8.png"
          radius="xl"
        />

        <div style={{ flex: 1 }}>
          <Text size="sm" fw={500}>
            CSK
          </Text>

          <Text c="dimmed" size="xs">
            cskcvarma13@gmail.com
          </Text>
        </div>
        <IconChevronRight className="IconChevronRight" />
      </Group>
    </UnstyledButton>
  );
};
