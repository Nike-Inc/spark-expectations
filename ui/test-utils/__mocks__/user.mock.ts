import { faker } from '@faker-js/faker';

export const createPlanMock = (): Plan => ({
  name: faker.company.name(),
  space: faker.number.int({ min: 1000, max: 10000 }),
  private_repos: faker.number.int({ max: 100 }),
  collaborators: faker.number.int({ max: 10 }),
});

export const createUserMock = (): User => ({
  login: faker.internet.userName(),
  id: faker.number.int(),
  avatar_url: faker.image.avatar(),
  subscriptions_url: faker.internet.url(),
  organizations_url: faker.internet.url(),
  name: faker.person.fullName(),
});

//
// export const createUserMock = (): User => ({
//   login: faker.internet.userName(),
//   id: faker.number.int(),
//   node_id: faker.string.uuid(),
//   avatar_url: faker.image.avatar(),
//   gravatar_id: '',
//   url: faker.internet.url(),
//   html_url: faker.internet.url(),
//   followers_url: faker.internet.url(),
//   following_url: faker.internet.url(),
//   gists_url: faker.internet.url(),
//   starred_url: faker.internet.url(),
//   subscriptions_url: faker.internet.url(),
//   organizations_url: faker.internet.url(),
//   repos_url: faker.internet.url(),
//   events_url: faker.internet.url(),
//   received_events_url: faker.internet.url(),
//   type: 'User',
//   site_admin: faker.datatype.boolean(),
//   name: faker.person.fullName(),
//   company: faker.company.name(),
//   blog: faker.internet.url(),
//   location: faker.location.secondaryAddress(),
//   email: faker.internet.email(),
//   hireable: faker.datatype.boolean(),
//   bio: faker.lorem.sentence(),
//   twitter_username: faker.internet.userName(),
//   public_repos: faker.number.int({ max: 100 }),
//   public_gists: faker.number.int({ max: 100 }),
//   followers: faker.number.int({ max: 1000 }),
//   following: faker.number.int({ max: 1000 }),
//   created_at: faker.date.past().toISOString(),
//   updated_at: faker.date.recent().toISOString(),
//   private_gists: faker.number.int({ max: 100 }),
//   total_private_repos: faker.number.int({ max: 100 }),
//   owned_private_repos: faker.number.int({ max: 100 }),
//   disk_usage: faker.number.int({ max: 10000 }),
//   collaborators: faker.number.int({ max: 10 }),
//   two_factor_authentication: faker.datatype.boolean(),
//   plan: createPlanMock(),
// });

export const useUserMock = () => ({
  data: createUserMock(),
  isLoading: false,
  isError: false,
});
