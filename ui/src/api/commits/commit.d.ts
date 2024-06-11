interface Author {
  name: string;
  email: string;
  date: string;
}

interface Tree {
  sha: string;
  url: string;
}

interface Verification {
  verified: boolean;
  reason: string;
  signature: string | null;
  payload: string | null;
}

interface CommitDetail {
  author: Author;
  committer: Author;
  message: string;
  tree: Tree;
  url: string;
  comment_count: number;
  verification: Verification;
}

interface Commit {
  sha: string;
  node_id: string;
  commit: CommitDetail;
}
