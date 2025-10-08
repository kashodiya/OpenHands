from typing import Any

from botocore.exceptions import ClientError

from openhands.core.logger import openhands_logger as logger
from openhands.integrations.codecommit.service.base import CodeCommitMixinBase
from openhands.integrations.service_types import (
    MicroagentContentResponse,
    MicroagentResponse,
    ResourceNotFoundError,
)


class CodeCommitResolverMixin(CodeCommitMixinBase):
    """
    Methods for resolving files and microagents in AWS CodeCommit repositories
    """

    async def _get_cursorrules_url(self, repository: str) -> str:
        """Get the URL for checking .cursorrules file."""
        # Extract repository name from full path if needed
        repo_name = repository.split('/')[-1] if '/' in repository else repository
        return f'{self.BASE_URL}/repositories/{repo_name}/files/.cursorrules'

    async def _get_microagents_directory_url(
        self, repository: str, microagents_path: str
    ) -> str:
        """Get the URL for checking microagents directory."""
        # Extract repository name from full path if needed
        repo_name = repository.split('/')[-1] if '/' in repository else repository
        return f'{self.BASE_URL}/repositories/{repo_name}/files/{microagents_path}'

    def _get_microagents_directory_params(self, microagents_path: str) -> dict | None:
        """Get parameters for the microagents directory request."""
        return None

    def _is_valid_microagent_file(self, item: dict | str) -> bool:
        """Check if an item represents a valid microagent file."""
        # AWS CodeCommit returns file names directly
        if isinstance(item, str):
            return item.endswith('.md')
        # Or it might return a dict with file information
        elif isinstance(item, dict) and 'name' in item:
            return item['name'].endswith('.md')
        return False

    def _get_file_name_from_item(self, item: dict | str) -> str:
        """Extract file name from directory item."""
        if isinstance(item, str):
            return item
        return item.get('name', '')

    def _get_file_path_from_item(self, item: dict | str, microagents_path: str) -> str:
        """Extract file path from directory item."""
        file_name = self._get_file_name_from_item(item)
        return f'{microagents_path}/{file_name}'

    async def get_microagent_content(
        self, repository: str, path: str
    ) -> MicroagentContentResponse:
        """Get the content of a microagent file.

        Args:
            repository: Repository name
            path: Path to the microagent file

        Returns:
            MicroagentContentResponse with content and triggers
        """
        try:
            # Extract repository name from full path if needed
            repo_name = repository.split('/')[-1] if '/' in repository else repository

            # Get the file content
            try:
                # Get the file content
                response = self.client.get_file(
                    repositoryName=repo_name,
                    filePath=path,
                )

                # Decode the file content
                content = response.get('fileContent', b'')
                if isinstance(content, bytes):
                    content = content.decode('utf-8')

                # Parse the microagent content
                return self._parse_microagent_content(content, path)

            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'FileDoesNotExistException':
                    raise ResourceNotFoundError(
                        f'File {path} not found in repository {repo_name}'
                    )
                raise self.handle_boto3_error(e)

        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def _fetch_cursorrules_content(self, repository: str) -> Any | None:
        """Fetch .cursorrules file content from the repository."""
        try:
            # Extract repository name from full path if needed
            repo_name = repository.split('/')[-1] if '/' in repository else repository

            # Get the file content
            response = self.client.get_file(
                repositoryName=repo_name,
                filePath='.cursorrules',
            )

            # Return the file content
            content = response.get('fileContent', b'')
            if isinstance(content, bytes):
                content = content.decode('utf-8')

            return content

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'FileDoesNotExistException':
                return None
            raise self.handle_boto3_error(e)

    async def _process_microagents_directory(
        self, repository: str, microagents_path: str
    ) -> list[MicroagentResponse]:
        """Process microagents directory and return list of microagent responses."""
        try:
            # Extract repository name from full path if needed
            repo_name = repository.split('/')[-1] if '/' in repository else repository

            microagents = []

            try:
                # List files in the directory
                # AWS CodeCommit doesn't have a direct API to list files in a directory
                # We need to use the git API to list the tree

                # First, get the default branch
                repo_info = self.client.get_repository(repositoryName=repo_name)

                default_branch = repo_info.get('repositoryMetadata', {}).get(
                    'defaultBranch', 'main'
                )

                # Get the latest commit on the default branch
                branch_info = self.client.get_branch(
                    repositoryName=repo_name, branchName=default_branch
                )

                commit_id = branch_info.get('branch', {}).get('commitId')

                # Get the tree for the commit
                # Unfortunately, AWS CodeCommit doesn't have a direct API to list files in a directory
                # We need to use a workaround by getting the folder content

                # Try to get the folder content
                try:
                    folder_content = self.client.get_folder(
                        repositoryName=repo_name,
                        folderPath=microagents_path,
                        commitSpecifier=commit_id,
                    )

                    # Process files in the folder
                    for file_info in folder_content.get('files', []):
                        file_name = file_info.get('absolutePath', '').split('/')[-1]
                        if file_name.endswith('.md'):
                            file_path = f'{microagents_path}/{file_name}'
                            microagents.append(
                                self._create_microagent_response(file_name, file_path)
                            )

                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    if error_code == 'FolderDoesNotExistException':
                        logger.info(
                            f'Microagents directory {microagents_path} not found in repository {repo_name}'
                        )
                    else:
                        raise self.handle_boto3_error(e)

            except ClientError as e:
                raise self.handle_boto3_error(e)

            return microagents

        except ClientError as e:
            raise self.handle_boto3_error(e)
