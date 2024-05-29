import { PersistStorage } from 'zustand/middleware';

export const customStorage: PersistStorage<any> = {
  getItem: (name) => {
    const item = localStorage.getItem(name);
    if (!item) return null;

    const parsedItem = JSON.parse(item);
    const now = new Date().getTime();

    if (parsedItem.expiry && now > parsedItem.expiry) {
      localStorage.removeItem(name);
      return null;
    }

    return parsedItem.value;
  },
  setItem: (name, value) => {
    const now = new Date().getTime();
    const item = {
      value,
      expiry: now + 6 * 60 * 60 * 1000, // 6 hours from now
    };
    localStorage.setItem(name, JSON.stringify(item));
  },
  removeItem: (name) => {
    localStorage.removeItem(name);
  },
};
