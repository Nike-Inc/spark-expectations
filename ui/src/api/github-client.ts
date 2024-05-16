import axios from 'axios';
import { useAuthStore } from '@/store';

export const gitHubClient = () => {
  //TODO: Is this a good practice?
  const { token } = useAuthStore.getState();

  return axios.create({
    baseURL: 'https://api.github.com/',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
  });
};
