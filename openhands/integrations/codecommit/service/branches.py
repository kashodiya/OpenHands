from botocore.exceptions import ClientError

from openhands.core.logger import openhands_logger as logger
from openhands.integrations.codecommit.service.base import CodeCommitMixinBase
from openhands.integrations.service_types import Branch, PaginatedBranchesResponse


class CodeCommitBranchesMixin(CodeCommitMixinBase):
    """
    Methods for interacting with AWS CodeCommit branches
    """

    async def get_branches(self, repository: str) -> list[Branch]:
        """Get branches for a repository.

        Args:
            repository: Repository name

        Returns:
            List of branches
        """
        paginated_response = await self.get_paginated_branches(repository)
        return paginated_response.branches

    async def get_paginated_branches(
        self, repository: str, page: int = 1, per_page: int = 100
    ) -> PaginatedBranchesResponse:
        """Get branches for a repository with pagination.

        Args:
            repository: Repository name
            page: Page number (1-indexed)
            per_page: Number of items per page

        Returns:
            PaginatedBranchesResponse with branches and pagination info
        """
        try:
            # Extract repository name from full path if needed
            repo_name = repository.split('/')[-1] if '/' in repository else repository

            # List branches
            response = self.client.list_branches(repositoryName=repo_name)

            branch_names = response.get('branches', [])
            next_token = response.get('nextToken')

            # Calculate pagination
            total_count = len(branch_names)
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page

            # Get branches for current page
            current_page_branch_names = (
                branch_names[start_idx:end_idx] if start_idx < total_count else []
            )

            # Get details for each branch
            branches = []
            for branch_name in current_page_branch_names:
                try:
                    # Get branch details
                    branch_info = self.client.get_branch(
                        repositoryName=repo_name, branchName=branch_name
                    )

                    commit_id = branch_info.get('branch', {}).get('commitId', '')

                    # Get commit details to get last push date
                    commit_info = self.client.get_commit(
                        repositoryName=repo_name, commitId=commit_id
                    )

                    # Format the date as ISO 8601
                    commit_date = (
                        commit_info.get('commit', {}).get('committer', {}).get('date')
                    )
                    last_push_date = (
                        commit_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                        if commit_date
                        else None
                    )

                    # Check if branch is protected
                    # AWS CodeCommit uses approval rules rather than branch protection
                    # We'll consider a branch protected if it has approval rules
                    try:
                        approval_rules = (
                            self.client.get_approval_rule_template_for_branch(
                                repositoryName=repo_name, branchName=branch_name
                            )
                        )
                        is_protected = (
                            len(approval_rules.get('approvalRuleTemplateNames', [])) > 0
                        )
                    except ClientError:
                        # No approval rules found
                        is_protected = False

                    branches.append(
                        Branch(
                            name=branch_name,
                            commit_sha=commit_id,
                            protected=is_protected,
                            last_push_date=last_push_date,
                        )
                    )
                except ClientError as e:
                    logger.warning(
                        f'Error getting details for branch {branch_name}: {e}'
                    )

            return PaginatedBranchesResponse(
                branches=branches,
                has_next_page=next_token is not None,
                current_page=page,
                per_page=per_page,
                total_count=total_count,
            )

        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def get_default_branch(self, repository: str) -> str:
        """Get the default branch for a repository.

        Args:
            repository: Repository name

        Returns:
            Name of the default branch
        """
        try:
            # Extract repository name from full path if needed
            repo_name = repository.split('/')[-1] if '/' in repository else repository

            # Get repository metadata
            response = self.client.get_repository(repositoryName=repo_name)

            # AWS CodeCommit uses 'defaultBranch' as the key
            default_branch = response.get('repositoryMetadata', {}).get('defaultBranch')

            return default_branch or 'main'  # Default to 'main' if not specified

        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def search_branches(
        self, repository: str, query: str, per_page: int = 30
    ) -> list[Branch]:
        """Search branches in a repository.

        Args:
            repository: Repository name
            query: Search query
            per_page: Number of items per page

        Returns:
            List of matching branches
        """
        try:
            # Get all branches
            paginated_response = await self.get_paginated_branches(
                repository, per_page=per_page
            )
            all_branches = paginated_response.branches

            # Filter branches by query
            matching_branches = [
                branch
                for branch in all_branches
                if query.lower() in branch.name.lower()
            ]

            return matching_branches

        except ClientError as e:
            raise self.handle_boto3_error(e)
