import { create } from 'zustand';
// import { persist } from 'zustand/middleware';

interface AuthState {
  token: string | null;
  username: string | null;
  setToken: (token: string | null) => void;
  setUserName: (username: string) => void;
}

/*
 * Coupling modals with state management.
 * Not sure if it's a good idea to do this.
 *
 * Keep an eye on this approach
 * Potential TODO
 * */
//
// export const useAuthStore = create(
//   persist<AuthState>(
//     (set: any) => ({
//       token: null,
//       username: null,
//       setToken: (token: string | null) => set(() => ({ token })),
//       setUserName: (username: string) => set(() => ({ username })),
//     }),
//     {
//       name: 'auth',
//     }
//   )
// );
// //
export const useAuthStore = create<AuthState>((set) => ({
  token: null,
  username: null,
  setToken: (token: string | null) => set(() => ({ token })),
  setUserName: (username: string) => set(() => ({ username })),
}));
