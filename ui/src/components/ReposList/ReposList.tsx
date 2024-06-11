import { Accordion, Avatar, Group, Text } from '@mantine/core';
import React, { lazy, Suspense } from 'react';
import { useRepos } from '@/api';
import { Loading } from '@/components';
import { useRepoStore } from '@/store';

const FilesList = lazy(() => import('@/components/FilesList/FilesList'));

interface AccordionLabelProps {
  label: string;
  image: string;
  description: string;
}

const AccordionLabel = ({ label, image, description }: AccordionLabelProps) => (
  <Group wrap="nowrap">
    <Avatar src={image} radius="sm" size="md" />
    <div>
      <Text>{label}</Text>
      <Text size="sm" c="dimmed" fw={400}>
        {description}
      </Text>
    </div>
  </Group>
);

export const ReposList = () => {
  const { data, isLoading, isError } = useRepos();
  const { selectRepo } = useRepoStore((state) => ({
    selectRepo: state.selectRepo,
  }));

  if (isLoading) {
    return <Loading />;
  }

  if (isError) {
    return <div>Error</div>;
  }

  const items = data?.map((repo: Repo) => (
    <Accordion.Item value={repo.name} key={repo.name}>
      <Accordion.Control onClick={() => selectRepo(repo)}>
        <AccordionLabel
          label={repo.name}
          image={repo.owner.avatar_url}
          description={repo.description}
        />
      </Accordion.Control>
      <Accordion.Panel>
        <Suspense fallback={<Loading />}>
          <FilesList selectedRepo={repo} />
        </Suspense>
      </Accordion.Panel>
    </Accordion.Item>
  ));

  return (
    <Accordion chevronPosition="right" variant="contained">
      {items}
    </Accordion>
  );
};
