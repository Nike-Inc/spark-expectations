import axios from 'axios';

export const gitHubClient = axios.create({
  baseURL: 'https://api.github.com/',
});
