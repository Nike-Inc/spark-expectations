import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { customStorage } from '../custom-store';

interface AuthState {
  token: string | null;
  username: string | null;
  setToken: (token: string | null) => void;
  setUserName: (username: string) => void;
}

export const useAuthStore = create(
  persist<AuthState>(
    (set: any) => ({
      token: null,
      username: null,
      setToken: (token: string | null) => set(() => ({ token })),
      setUserName: (username: string) => set(() => ({ username })),
    }),
    {
      name: 'auth',
      storage: customStorage,
    }
  )
);
// //
// export const useAuthStore = create<AuthState>((set) => ({
//   token: null,
//   username: null,
//   setToken: (token: string | null) => set(() => ({ token })),
//   setUserName: (username: string) => set(() => ({ username })),
// }));
