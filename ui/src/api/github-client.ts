import axios from 'axios';

export const gitHubClient = () =>
  axios.create({
    baseURL: 'https://api.github.com/',
    headers: {
      'Content-Type': 'application/json',
    },
  });

const GithubLoginCredentials = {
  clientId: import.meta.env.VITE_GITHUB_CLIENT_ID,
  loginUrl: import.meta.env.VITE_GITHUB_CLIENT_USER_IDENTITY_URL,
  redirectUri: import.meta.env.VITE_GITHUB_REDIRECT_URI,
  scopes: import.meta.env.VITE_GITHUB_SCOPES,
};

export const githubLogin = () => {
  const { loginUrl, clientId, redirectUri, scopes } = GithubLoginCredentials;
  window.location.href = `${loginUrl}?client_id=${clientId}&redirect_uri=${redirectUri}&scope=${scopes}`;
};
