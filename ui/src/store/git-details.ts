import { create } from 'zustand';
import { persist } from 'zustand/middleware';

type gitDetails = {
  baseUrl: string;
  token: string;
  addBaseUrl: (baseUrl: string) => void;
  addToken: (token: string) => void;
};

export const useGitDetails = create(
  persist<gitDetails>(
    (set: any) => ({
      baseUrl: '',
      token: '',
      addBaseUrl: (baseUrl: string) => set(() => ({ baseUrl })),
      addToken: (token: string) => set(() => ({ token })),
    }),
    {
      name: 'git-details',
    }
  )
);
