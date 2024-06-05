import { List, Paper, ScrollArea, Text, Title, Tooltip } from '@mantine/core';
import { useRepoStore } from '@/store';
import { useRepo } from '@/api';
import { Loading } from '@/components';

export const FilesList = () => {
  const { selectedRepo, selectFile } = useRepoStore((state) => ({
    selectedRepo: state.selectedRepo,
    selectFile: state.selectFile,
  }));

  const { data, isLoading, isError } = useRepo(
    selectedRepo?.owner?.login,
    selectedRepo?.name,
    '',
    true
  );

  if (isLoading) {
    return <Loading />;
  }

  if (isError) {
    return <div>Error</div>;
  }

  return (
    <Paper shadow="sm" radius="md" p="md" withBorder>
      <Title order={3} style={{ marginBottom: 20 }}>
        YAML Files
      </Title>
      <ScrollArea style={{ height: 300 }}>
        <List spacing="sm" size="sm" center>
          {data?.map((file: any, index: number) => (
            <List.Item key={index} onClick={() => selectFile(file)}>
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
//
// const NestedList = ({ item }) => {
//   const { selectedRepo } = useRepoStore();
//   const [isOpen, setIsOpen] = useState(false);
//   const hasChildren = item.type === 'dir';
//
//   const { data, isLoading, isError } = useRepo(
//     selectedRepo?.owner?.login,
//     selectedRepo?.name,
//     item.path,
//     isOpen && hasChildren
//   );
//
//   const toggle = () => setIsOpen(!isOpen);
//
//   return (
//     <div>
//       <Group position="apart" style={{ cursor: 'pointer', alignItems: 'center' }} onClick={toggle}>
//         <Text>{item.name}</Text>
//         {hasChildren && (
//           <ThemeIcon size="sm">
//             <IconChevronRight style={{ transform: isOpen ? 'rotate(90deg)' : 'rotate(0deg)' }} />
//           </ThemeIcon>
//         )}
//       </Group>
//       {isLoading && <Loader />}
//       {isError && <Text color="red">Failed to load data</Text>}
//       {isOpen && hasChildren && (
//         <Collapse in={isOpen}>
//           <Group direction="column" spacing="xs">
//             {data?.map((child: any) => <NestedList key={child.sha} item={child} />)}
//           </Group>
//         </Collapse>
//       )}
//     </div>
//   );
// };
