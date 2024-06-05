import { create } from 'zustand';

interface RepoState {
  selectedRepo: Repo | null;
  selectRepo: (repo: Repo) => void;
  selectedFile: any;
  selectFile: (file: any) => void;
}

export const useRepoStore = create<RepoState>((set) => ({
  selectedRepo: null,
  selectRepo: (repo: Repo) => set(() => ({ selectedRepo: repo })),
  selectedFile: null,
  selectFile: (file: any) => set(() => ({ selectedFile: file })),
}));
