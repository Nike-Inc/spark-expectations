import { create } from 'zustand';

interface AuthState {
  token: string | null;
  isModalOpen: boolean;
  setToken: (token: string) => void;
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

export const useAuthStore = create<AuthState>((set) => ({
  token: null,
  isModalOpen: false,
  setToken: (token: string) => set(() => ({ token })),
  openModal: () => set(() => ({ isModalOpen: true })),
  closeModal: () => set(() => ({ isModalOpen: false })),
}));
