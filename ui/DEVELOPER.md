# Developer Setup Guide

## Setting Up GitHub OAuth

To set up GitHub OAuth for this application, you will need to configure several environment variables in a `.env.local` file. Below are the steps to get you started.

1. **Create a GitHub OAuth App**:
    - Go to [GitHub Developer Settings](https://github.com/settings/developers).
    - Click on "New OAuth App".
    - Fill in the details with your application's information:
        - **Application name**: Your app's name
        - **Homepage URL**: `http://localhost:3000`
        - **Authorization callback URL**: `http://localhost:3000/oauth/callback`
    - After creating the app, you will get a **Client ID** and **Client Secret**.

2. **Set Environment Variables**:
    - Create a file named `.env.local` in the root directory of your project.
    - Copy and paste the following template into `.env.local` and fill in the values with your GitHub OAuth credentials and URLs.

    ```plaintext
    VITE_GITHUB_CLIENT_ID=your_client_id
    VITE_GITHUB_REDIRECT_URI=http://localhost:3000/oauth/callback
    VITE_GITHUB_CLIENT_SECRET=your_client_secret
    VITE_GITHUB_CLIENT_USER_IDENTITY_URL=https://github.com/login/oauth/authorize
    VITE_GITHUB_LOGIN_URL=https://github.com/login/oauth/access_token
    VITE_GITHUB_CORS_LOGIN_URL=https://cors-anywhere.herokuapp.com/https://github.com/login/oauth/authorize
    VITE_GITHUB_SCOPES=repo
    VITE_STORAGE_EXPIRY_DURATION=21600000
    ```

3. **Run the Application**:
    - Make sure you have all dependencies installed.
    - Start the development server:

    ```sh
    npm install
    npm run dev
    ```

## Additional Notes

- The `VITE_GITHUB_CORS_LOGIN_URL` is used to handle CORS issues during development. You might need to adjust this depending on your deployment environment.
- The `VITE_STORAGE_EXPIRY_DURATION` is set to 6 hours (21600000 milliseconds).

By following these steps, you should be able to set up and run the application with GitHub OAuth configured.

Happy coding!
