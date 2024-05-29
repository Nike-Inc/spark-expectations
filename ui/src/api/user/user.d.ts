// Here is the type that will be User across the codebase,
// If integrating with a different git provider,
// you will need to update the type of the user object.
type User = Pick<
  GitHubUser,
  'avatar_url' | 'name' | 'login' | 'id' | 'organizations_url' | 'subscriptions_url'
>;
