import { create } from 'zustand';

interface RepoState {
  selectedRepo: Repo | null;
  selectRepo: (repo: Repo) => void;
  selectedFile: any;
  selectFile: (file: any) => void;
  selectedBranch: Branch | null;
  selectBranch: (branch: Branch) => void;
}

export const useRepoStore = create<RepoState>((set) => ({
  selectedRepo: null,
  selectRepo: (repo: Repo) => set(() => ({ selectedRepo: repo })),
  selectedFile: null,
  selectFile: (file: any) => set(() => ({ selectedFile: file })),
  selectedBranch: null,
  selectBranch: (branch: Branch) => set(() => ({ selectedBranch: branch })),
}));
