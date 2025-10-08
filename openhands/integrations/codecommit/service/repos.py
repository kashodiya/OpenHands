from botocore.exceptions import ClientError

from openhands.core.logger import openhands_logger as logger
from openhands.integrations.codecommit.service.base import CodeCommitMixinBase
from openhands.integrations.service_types import OwnerType, ProviderType, Repository
from openhands.server.types import AppMode


class CodeCommitReposMixin(CodeCommitMixinBase):
    """
    Methods for interacting with AWS CodeCommit repositories
    """

    async def search_repositories(
        self, query: str, per_page: int, sort: str, order: str, public: bool
    ) -> list[Repository]:
        """Search for repositories in AWS CodeCommit.

        Args:
            query: Search query
            per_page: Number of results per page
            sort: Sort field
            order: Sort order
            public: Whether to search for public repositories only

        Returns:
            List of repositories matching the search criteria
        """
        try:
            # AWS CodeCommit doesn't have a search API, so we'll list all repositories
            # and filter them client-side
            response = self.client.list_repositories()
            repositories = response.get('repositories', [])

            # Filter repositories by name if query is provided
            if query:
                repositories = [
                    repo
                    for repo in repositories
                    if query.lower() in repo.get('repositoryName', '').lower()
                ]

            # AWS CodeCommit doesn't have public/private distinction
            # All repositories are private to the AWS account

            # Get details for each repository
            result = []
            for repo in repositories[:per_page]:  # Limit to per_page
                repo_name = repo.get('repositoryName')
                try:
                    # Get repository metadata
                    repo_info = self.client.get_repository(repositoryName=repo_name)

                    metadata = repo_info.get('repositoryMetadata', {})

                    # Get the default branch
                    default_branch = metadata.get('defaultBranch')

                    # Format the last modified date
                    last_modified = metadata.get('lastModifiedDate')
                    pushed_at = (
                        last_modified.strftime('%Y-%m-%dT%H:%M:%SZ')
                        if last_modified
                        else None
                    )

                    result.append(
                        Repository(
                            id=metadata.get('repositoryId', ''),
                            full_name=repo_name,
                            git_provider=ProviderType.CODECOMMIT,
                            is_public=False,  # AWS CodeCommit repositories are always private
                            stargazers_count=0,  # AWS CodeCommit doesn't have stars
                            pushed_at=pushed_at,
                            owner_type=OwnerType.ORGANIZATION,  # AWS account is more like an organization
                            main_branch=default_branch,
                        )
                    )
                except ClientError as e:
                    logger.warning(
                        f'Error getting details for repository {repo_name}: {e}'
                    )

            return result

        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def get_all_repositories(
        self, sort: str, app_mode: AppMode
    ) -> list[Repository]:
        """Get all repositories for the authenticated user.

        Args:
            sort: Sort field
            app_mode: Application mode

        Returns:
            List of all repositories
        """
        try:
            # List all repositories
            response = self.client.list_repositories()
            repositories = response.get('repositories', [])

            # Get details for each repository
            result = []
            for repo in repositories:
                repo_name = repo.get('repositoryName')
                try:
                    # Get repository metadata
                    repo_info = self.client.get_repository(repositoryName=repo_name)

                    metadata = repo_info.get('repositoryMetadata', {})

                    # Get the default branch
                    default_branch = metadata.get('defaultBranch')

                    # Format the last modified date
                    last_modified = metadata.get('lastModifiedDate')
                    pushed_at = (
                        last_modified.strftime('%Y-%m-%dT%H:%M:%SZ')
                        if last_modified
                        else None
                    )

                    result.append(
                        Repository(
                            id=metadata.get('repositoryId', ''),
                            full_name=repo_name,
                            git_provider=ProviderType.CODECOMMIT,
                            is_public=False,  # AWS CodeCommit repositories are always private
                            stargazers_count=0,  # AWS CodeCommit doesn't have stars
                            pushed_at=pushed_at,
                            owner_type=OwnerType.ORGANIZATION,  # AWS account is more like an organization
                            main_branch=default_branch,
                        )
                    )
                except ClientError as e:
                    logger.warning(
                        f'Error getting details for repository {repo_name}: {e}'
                    )

            return result

        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def get_paginated_repos(
        self,
        page: int,
        per_page: int,
        sort: str,
        installation_id: str | None,
        query: str | None = None,
    ) -> list[Repository]:
        """Get a page of repositories.

        Args:
            page: Page number (1-indexed)
            per_page: Number of items per page
            sort: Sort field
            installation_id: Installation ID (not used for AWS CodeCommit)
            query: Search query

        Returns:
            List of repositories for the current page
        """
        try:
            # List all repositories
            response = self.client.list_repositories()
            repositories = response.get('repositories', [])

            # Filter repositories by name if query is provided
            if query:
                repositories = [
                    repo
                    for repo in repositories
                    if query.lower() in repo.get('repositoryName', '').lower()
                ]

            # Calculate pagination
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page

            # Get repositories for current page
            current_page_repos = (
                repositories[start_idx:end_idx] if start_idx < len(repositories) else []
            )

            # Get details for each repository
            result = []
            for repo in current_page_repos:
                repo_name = repo.get('repositoryName')
                try:
                    # Get repository metadata
                    repo_info = self.client.get_repository(repositoryName=repo_name)

                    metadata = repo_info.get('repositoryMetadata', {})

                    # Get the default branch
                    default_branch = metadata.get('defaultBranch')

                    # Format the last modified date
                    last_modified = metadata.get('lastModifiedDate')
                    pushed_at = (
                        last_modified.strftime('%Y-%m-%dT%H:%M:%SZ')
                        if last_modified
                        else None
                    )

                    result.append(
                        Repository(
                            id=metadata.get('repositoryId', ''),
                            full_name=repo_name,
                            git_provider=ProviderType.CODECOMMIT,
                            is_public=False,  # AWS CodeCommit repositories are always private
                            stargazers_count=0,  # AWS CodeCommit doesn't have stars
                            pushed_at=pushed_at,
                            owner_type=OwnerType.ORGANIZATION,  # AWS account is more like an organization
                            main_branch=default_branch,
                        )
                    )
                except ClientError as e:
                    logger.warning(
                        f'Error getting details for repository {repo_name}: {e}'
                    )

            return result

        except ClientError as e:
            raise self.handle_boto3_error(e)
