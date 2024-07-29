// import { useMutation, useQueryClient } from '@tanstack/react-query';
// import { apiClient } from '@/api';
//
// const checkBranchExists = async (owner, repo, branch) => {
//   try {
//     const response = await apiClient.get(`/repos/${owner}/${repo}/branches/${branch}`);
//     return response.data;
//   } catch (error) {
//     if (error.response && error.response.status === 404) {
//       return false;
//     }
//     throw error;
//   }
// };
//
// const gitHubCreateBranch = async (owner, repo, baseBranch, newBranch) => {
//   const response = await apiClient.post(`/repos/${owner}/${repo}/git/refs`, {
//     ref: `refs/heads/${newBranch}`,
//     sha: baseBranch,
//   });
//   console.log(response.data);
//
//   return response.data;
// };
//
// const getRefSha = async (owner, repo, ref) => {
//   const response = await apiClient.get(`/repos/${owner}/${repo}/git/refs/${ref}`);
//   console.log(response.data);
//   return response.data.object.sha;
// };
//
// const verifySha = async (owner, repo, sha) => {
//   const response = await apiClient.get(`/repos/${owner}/${repo}/commits/${sha}`);
//   console.log(response.data);
//   return response.data.sha;
// };
//
// export const createBranch = async ({ owner, repo, baseBranch, newBranch }) => {
//   const branchExists = await checkBranchExists(owner, repo, newBranch);
//
//   if (branchExists) {
//     return { message: 'Branch already exists', branch: newBranch };
//   }
//
//   const baseBranchSha = await getRefSha(owner, repo, `heads/${baseBranch}`);
//   const verifiedSha = await verifySha(owner, repo, `heads/${baseBranch}`);
//
//   const newBranchData = await gitHubCreateBranch(owner, repo, verifiedSha, newBranch);
//   return { message: 'Branch created', branch: newBranchData.ref };
// };
//
// export const useCreateBranch = () => {
//   const queryClient = useQueryClient();
//
//   return useMutation(createBranch, {
//     onSuccess: (data) => {
//       console.log('Mutation successful:', data);
//       queryClient.invalidateQueries('branches');
//     },
//     onError: (error) => {
//       console.error('Mutation failed:', error);
//     },
//   });
// };
//
// // export default useCreateBranch;
