import axios from 'axios';
import { useGitDetails } from '@/store';

export const gitApi = axios.create({
  baseURL: useGitDetails.getState().baseUrl,
  timeout: 1000,
  headers: {
    Authorization: `Bearer ${useGitDetails.getState().token}`,
    'Content-Type': 'application/json',
  },
});

// TODO: Auth token in not updating when it changes.
useGitDetails.subscribe((state) => {
  gitApi.defaults.baseURL = state.baseUrl;
  gitApi.defaults.headers.common.Authorization = `Bearer ${state.token}`;
});
