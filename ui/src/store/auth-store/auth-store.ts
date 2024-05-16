import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface AuthState {
  token: string | null;
  isModalOpen: boolean;
  username: string | null;
  setToken: (token: string | null) => void;
  setUserName: (username: string) => void;
  openModal: () => void;
  closeModal: () => void;
}

/*
 * Coupling modals with state management.
 * Not sure if it's a good idea to do this.
 *
 * Keep an eye on this approach
 * Potential TODO
 * */

export const useAuthStore = create(
  persist<AuthState>(
    (set: any) => ({
      token: null,
      isModalOpen: false,
      username: null,
      setToken: (token: string | null) => set(() => ({ token })),
      setUserName: (username: string) => set(() => ({ username })),
      openModal: () => set(() => ({ isModalOpen: true })),
      closeModal: () => set(() => ({ isModalOpen: false })),
    }),
    {
      name: 'auth',
    }
  )
);

// export const useAuthStore = create<AuthState>((set) => ({
//   token: null,
//   isModalOpen: false,
//   username: null,
//   setToken: (token: string) => set(() => ({ token })),
//   setUserName: (username: string) => set(() => ({ username })),
//   openModal: () => set(() => ({ isModalOpen: true })),
//   closeModal: () => set(() => ({ isModalOpen: false })),
// }));
