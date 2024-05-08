import {
  Combobox,
  ComboboxChevron,
  ComboboxDropdown,
  ComboboxEmpty,
  ComboboxOption,
  ComboboxOptions,
  ComboboxTarget,
  InputBase,
  InputPlaceholder,
  ScrollArea,
  ScrollAreaAutosize,
  TextInput,
  useCombobox,
} from '@mantine/core';
import { useState } from 'react';

const groceries = [
  '🍎 Apples',
  '🍌 Bananas',
  '🥦 Broccoli',
  '🥕 Carrots',
  '🍫 Chocolate',
  '🍇 Grapes',
  '🍋 Lemon',
  '🥬 Lettuce',
  '🍄 Mushrooms',
  '🍊 Oranges',
  '🥔 Potatoes',
  '🍅 Tomatoes',
  '🥚 Eggs',
  '🥛 Milk',
  '🍞 Bread',
  '🍗 Chicken',
  '🍔 Hamburger',
  '🧀 Cheese',
  '🥩 Steak',
  '🍟 French Fries',
  '🍕 Pizza',
  '🥦 Cauliflower',
  '🥜 Peanuts',
  '🍦 Ice Cream',
  '🍯 Honey',
  '🥖 Baguette',
  '🍣 Sushi',
  '🥝 Kiwi',
  '🍓 Strawberries',
];

export const ReposList = () => {
  const combobox = useCombobox({
    scrollBehavior: 'smooth',
    onDropdownClose: () => combobox.resetSelectedOption(),
  });

  const [value, setValue] = useState<string>('');

  const shouldFilterOptions = !groceries.some((item) => item === value);
  const filteredOptions = shouldFilterOptions
    ? groceries.filter((item) => item.toLowerCase().includes(value.toLowerCase().trim()))
    : groceries;

  const options = filteredOptions.map((item) => (
    <ComboboxOption value={item} key={item}>
      {item}
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
          placeholder="pick a grocery or type anything"
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
            {options.length === 0 ? <ComboboxEmpty>Nothing Found</ComboboxEmpty> : options}
          </ScrollAreaAutosize>
        </ComboboxOptions>
      </ComboboxDropdown>
    </Combobox>
  );
};
