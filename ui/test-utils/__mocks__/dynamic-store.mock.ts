import { vi } from 'vitest';

export const createDynamicMockStore = (initialState: any) =>
  new Proxy(initialState, {
    get: (target, prop, receiver) => {
      // Automatically create a mock function if the property is not in the state and is a function call
      if (typeof target[prop] === 'undefined') {
        target[prop] = vi.fn(() => {
          // eslint-disable-next-line no-console
          console.log(`Called ${String(prop)} with no predefined behavior.`);
        });
      }
      return Reflect.get(target, prop, receiver);
    },
    set: (target, prop, value) => {
      target[prop] = value;
      return true;
    },
  });
