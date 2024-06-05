import { Avatar, NavLink } from '@mantine/core';
import { useRepos } from '@/api';
import { Loading } from '@/components';
import { useRepoStore } from '@/store';

export const ReposList = () => {
  const { data, isLoading, isError } = useRepos();
  const { selectedRepo, selectRepo } = useRepoStore();

  if (isLoading) {
    return <Loading />;
  }

  if (isError) {
    return <div>Error</div>;
  }

  return (
    <>
      {data?.map((repo: Repo) => (
        <NavLink
          label={repo.name}
          leftSection={<Avatar src={repo.owner.avatar_url} size="xs" radius="xl" />}
          key={repo.name}
          onClick={() => selectRepo(repo)}
          active={selectedRepo?.name === repo.name}
        />
      ))}
    </>
  );
};
