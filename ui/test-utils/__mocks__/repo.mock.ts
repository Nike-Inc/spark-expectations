import { faker } from '@faker-js/faker';

const createSecurityStatusMock = (): SecurityStatus => ({
  status: faker.helpers.arrayElement(['enabled', 'disabled']),
});

const createSecurityAndAnalysisMock = (): SecurityAndAnalysis => ({
  advanced_security: createSecurityStatusMock(),
  secret_scanning: createSecurityStatusMock(),
  secret_scanning_push_protection: createSecurityStatusMock(),
});

const createPermissionsMock = (): Permissions =>
  <Permissions>{
    admin: faker.datatype.boolean(),
    push: faker.datatype.boolean(),
    pull: faker.datatype.boolean(),
  };

const createOwnerMock = (): Owner => ({
  login: faker.internet.userName(),
  id: faker.number.int(),
  node_id: faker.string.uuid(),
  avatar_url: faker.image.avatar(),
  gravatar_id: '',
  url: faker.internet.url(),
  html_url: faker.internet.url(),
  followers_url: faker.internet.url(),
  following_url: faker.internet.url(),
  gists_url: faker.internet.url(),
  starred_url: faker.internet.url(),
  subscriptions_url: faker.internet.url(),
  organizations_url: faker.internet.url(),
  repos_url: faker.internet.url(),
  events_url: faker.internet.url(),
  received_events_url: faker.internet.url(),
  type: 'User',
  site_admin: faker.datatype.boolean(),
});

export const createRepoMock = (): Repos =>
  <Repos>{
    id: faker.number.int(),
    node_id: faker.string.uuid(),
    name: faker.company.name(),
    full_name: `${faker.company.name()}/${faker.internet.userName()}`,
    owner: createOwnerMock(),
    private: faker.datatype.boolean(),
    html_url: faker.internet.url(),
    description: faker.lorem.sentence(),
    fork: faker.datatype.boolean(),
    url: faker.internet.url(),
    permissions: createPermissionsMock(),
    security_and_analysis: createSecurityAndAnalysisMock(),
    // Populate other URLs and properties as needed...
  };
