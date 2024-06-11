import { List, Paper, ScrollArea, Text, Tooltip } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useRepoStore } from '@/store';
import { useRepo } from '@/api';
import { Loading } from '@/components';
import { getBranchFn } from '@/api/branches/github/use-branch';

interface FilesListProps {
  selectedRepo: Repo;
  onError?: (repoName: string, hasError: boolean) => void;
}

const FilesList = ({ selectedRepo }: FilesListProps) => {
  const { selectFile, selectBranch } = useRepoStore((state) => ({
    selectFile: state.selectFile,
    selectBranch: state.selectBranch,
  }));

  const { data, isLoading, isError } = useRepo(
    selectedRepo?.owner?.login,
    selectedRepo?.name,
    '',
    true
  );

  const handleCreateBranch = (file: any) => {
    getBranchFn()
      .then((branch: any) => {
        selectFile(file);
        selectBranch(branch);
      })
      .catch(() => {
        notifications.show({
          message: 'Error creating branch',
          color: 'red',
          title: 'Error',
        });
      });
  };

  if (isLoading) {
    return <Loading />;
  }

  if (isError) {
    return <div>Error</div>;
  }

  if (data && data.length === 0) {
    return <Text>No YAML files found</Text>;
  }

  return (
    <Paper shadow="sm" radius="md" p="md" withBorder>
      <ScrollArea style={{ height: 100 }}>
        <List spacing="sm" size="sm" center>
          {data?.map((file: any, index: number) => (
            <List.Item key={index} onClick={() => handleCreateBranch(file)}>
              <Tooltip label={file.path} position="bottom" withArrow>
                <Text style={{ cursor: 'pointer' }}>{file.name}</Text>
              </Tooltip>
            </List.Item>
          ))}
        </List>
      </ScrollArea>
    </Paper>
  );
};

export default FilesList;
