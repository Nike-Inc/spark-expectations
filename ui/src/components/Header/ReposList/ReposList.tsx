import {
  Alert,
  Combobox,
  ComboboxDropdown,
  ComboboxEmpty,
  ComboboxOption,
  ComboboxOptions,
  ComboboxTarget,
  ScrollAreaAutosize,
  Skeleton,
  TextInput,
  useCombobox,
} from '@mantine/core';
import { useState } from 'react';
import { useRepos } from '@/api';

export const ReposList = () => {
  const combobox = useCombobox({
    scrollBehavior: 'smooth',
    onDropdownClose: () => combobox.resetSelectedOption(),
  });

  const { data, error, isLoading } = useRepos();

  const [value, setValue] = useState<string>('');

  if (error) {
    return <ErrorComponent />;
  }

  if (isLoading) {
    return <LoadingSkeleton />;
  }

  const shouldFilterOptions = !data?.some((item) => item.name === value);
  const filteredOptions = shouldFilterOptions
    ? data?.filter((item) => item.name.toLowerCase().includes(value.toLowerCase().trim()))
    : data;

  const options = filteredOptions?.map((item: any) => (
    <ComboboxOption value={item.name} key={item.name}>
      {item.name}
    </ComboboxOption>
  ));

  return (
    <Combobox
      store={combobox}
      withinPortal={false}
      transitionProps={{ duration: 200, transition: 'pop' }}
      onOptionSubmit={(val) => {
        setValue(val);
        combobox.closeDropdown();
      }}
    >
      <ComboboxTarget>
        <TextInput
          placeholder="pick a repo or type anything"
          value={value}
          onChange={(e) => {
            setValue(e.currentTarget.value);
            combobox.openDropdown();
            combobox.updateSelectedOptionIndex();
          }}
          onClick={() => combobox.openDropdown()}
          onFocus={() => combobox.openDropdown()}
          onBlur={() => combobox.closeDropdown()}
        />
      </ComboboxTarget>

      <ComboboxDropdown>
        <ComboboxOptions>
          <ScrollAreaAutosize mah={200} type="scroll">
            {options?.length === 0 ? <ComboboxEmpty>Nothing Found</ComboboxEmpty> : options}
          </ScrollAreaAutosize>
        </ComboboxOptions>
      </ComboboxDropdown>
    </Combobox>
  );
};

const LoadingSkeleton = () => (
  <div style={{ padding: '10px' }} data-testid="loading-user-menu">
    <Skeleton height={20} radius="xl" animate />
    <Skeleton height={20} radius="xl" animate style={{ marginTop: '10px' }} />
    <Skeleton height={20} radius="xl" animate style={{ marginTop: '10px' }} />
  </div>
);

const ErrorComponent = () => (
  <Alert title="Error" color="red">
    Error!
  </Alert>
);
