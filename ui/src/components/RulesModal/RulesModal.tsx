import { Modal, Button, Textarea, Group, Stack } from '@mantine/core';
import { useForm } from '@mantine/form';

interface RulesModalProps {
  isOpen: boolean;
  onClose: () => void;
  rowData: any;
  onSave: (values: any) => void;
}

export const RulesModal = ({ isOpen, onClose, rowData, onSave }: RulesModalProps) => {
  const form = useForm({
    initialValues: rowData || {},
  });

  const handleSubmit = (values: any) => {
    onSave(values);
    onClose();
  };

  return (
    <Modal opened={isOpen} onClose={onClose} title="Edit Row Data">
      <form onSubmit={form.onSubmit(handleSubmit)}>
        <Stack>
          {Object.entries(rowData).map(([key]) => (
            <Textarea
              key={key}
              label={key.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())}
              {...form.getInputProps(key)}
            />
          ))}
          <Group p="right">
            <Button type="submit">Save</Button>
          </Group>
        </Stack>
      </form>
    </Modal>
  );
};
