import axios from 'axios';
import { useQuery } from '@tanstack/react-query';

export const exchangeGithubCodeForToken = async (code: string | null) => {
  const data = {
    client_id: import.meta.env.VITE_GITHUB_CLIENT_ID,
    client_secret: import.meta.env.VITE_GITHUB_CLIENT_SECRET,
    code,
    redirectUri: import.meta.env.VITE_GITHUB_REDIRECT_URI,
  };
  const response = await axios.post('/login/oauth/access_token', data, {
    headers: {
      Accept: 'application/json',
    },
  });
  return response.data as OAuth;
};

export const useOAuth = ({ code }: { code: string | null }) =>
  useQuery({
    queryKey: ['Github', 'oauth', code],
    queryFn: () => exchangeGithubCodeForToken(code),
    retry: 1,
  });
