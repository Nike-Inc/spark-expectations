import React from 'react';
import { Text, Title, Card, Divider, Group } from '@mantine/core';
import { useCommits } from '@/api/commits/github/use-commits';
import { Loading } from '@/components';
import { useRepoStore } from '@/store';

export const CommitsList = () => {
  const { selectedBranch } = useRepoStore((state) => ({ selectedBranch: state.selectedBranch }));
  const { data, error, isLoading } = useCommits(selectedBranch?.name, !!selectedBranch);
  if (isLoading) {
    return <Loading />;
  }

  if (error) {
    return <Text color="red">Error: {error.message}</Text>;
  }

  const commits: Commit[] = data || [];

  if (commits.length === 0) {
    return <></>;
  }

  return (
    <Card shadow="sm" padding="lg">
      <Title order={2}>Commits in {selectedBranch?.name}</Title>
      <Divider my="sm" />
      {commits.map((commit: Commit, index) => (
        <div key={index}>
          <Group p="apart">
            <Text w={500}>{commit.commit.message}</Text>
            <Text size="sm" color="dimmed">
              {commit.commit.date}
            </Text>
          </Group>
          <Text size="sm" color="dimmed">
            {commit.commit.author.name}
          </Text>
          {index < commits.length - 1 && <Divider my="sm" />}
        </div>
      ))}
    </Card>
  );
};
