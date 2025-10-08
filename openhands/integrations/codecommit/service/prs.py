from botocore.exceptions import ClientError

from openhands.core.logger import openhands_logger as logger
from openhands.integrations.codecommit.service.base import CodeCommitMixinBase


class CodeCommitPRsMixin(CodeCommitMixinBase):
    """
    Methods for interacting with AWS CodeCommit pull requests
    """

    async def create_pr(
        self,
        repo_name: str,
        source_branch: str,
        target_branch: str,
        title: str,
        body: str | None = None,
        draft: bool = False,
        labels: list[str] | None = None,
    ) -> str:
        """Creates a pull request in AWS CodeCommit

        Args:
            repo_name: The name of the repository
            source_branch: The name of the branch where your changes are implemented
            target_branch: The name of the branch you want the changes pulled into
            title: The title of the pull request
            body: The body/description of the pull request (optional)
            draft: Whether to create the PR as a draft (optional, not supported in CodeCommit)
            labels: A list of labels to apply to the pull request (optional, not supported in CodeCommit)

        Returns:
            - PR URL when successful
            - Error message when unsuccessful
        """
        try:
            # Extract repository name from full path if needed
            repository_name = (
                repo_name.split('/')[-1] if '/' in repo_name else repo_name
            )

            # Set default body if none provided
            if not body:
                body = f'Merging changes from {source_branch} into {target_branch}'

            # Create the pull request
            response = self.client.create_pull_request(
                title=title,
                description=body,
                targets=[
                    {
                        'repositoryName': repository_name,
                        'sourceReference': source_branch,
                        'destinationReference': target_branch,
                    }
                ],
                # AWS CodeCommit doesn't support draft PRs, so we ignore the draft parameter
            )

            # Get the pull request ID
            pr_id = response['pullRequest']['pullRequestId']

            # AWS CodeCommit doesn't support labels directly
            # If labels are provided, we could add them as comments or in the description
            if labels and len(labels) > 0:
                labels_text = 'Labels: ' + ', '.join(labels)
                try:
                    self.client.post_comment_for_pull_request(
                        pullRequestId=pr_id,
                        repositoryName=repository_name,
                        beforeCommitId=response['pullRequest']['revisionId'],
                        afterCommitId=response['pullRequest']['revisionId'],
                        content=labels_text,
                    )
                except ClientError as e:
                    logger.warning(f'Failed to add labels as comment: {e}')

            # Construct the URL to the pull request
            # Format: https://console.aws.amazon.com/codesuite/codecommit/repositories/{repository}/pull-requests/{pr_id}/details
            pr_url = f'https://console.aws.amazon.com/codesuite/codecommit/repositories/{repository_name}/pull-requests/{pr_id}/details?region={self.REGION}'

            return pr_url

        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def get_pr_details(self, repository: str, pr_number: int) -> dict:
        """Get detailed information about a specific pull request

        Args:
            repository: Repository name
            pr_number: The pull request ID

        Returns:
            Raw AWS CodeCommit API response for the pull request
        """
        try:
            # Extract repository name from full path if needed
            repository.split('/')[-1] if '/' in repository else repository

            # Get pull request details
            response = self.client.get_pull_request(pullRequestId=str(pr_number))

            return response['pullRequest']

        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def is_pr_open(self, repository: str, pr_number: int) -> bool:
        """Check if an AWS CodeCommit PR is still active (not closed/merged).

        Args:
            repository: Repository name
            pr_number: The PR number to check

        Returns:
            True if PR is active (open), False if closed/merged
        """
        try:
            pr_details = await self.get_pr_details(repository, pr_number)

            # AWS CodeCommit PR status values: 'OPEN', 'CLOSED'
            if 'pullRequestStatus' in pr_details:
                return pr_details['pullRequestStatus'] == 'OPEN'

            # If we can't determine the state, assume it's active (safer default)
            logger.warning(
                f'Could not determine AWS CodeCommit PR status for {repository}#{pr_number}. '
                f'Response keys: {list(pr_details.keys())}. Assuming PR is active.'
            )
            return True

        except Exception as e:
            logger.warning(
                f'Could not determine AWS CodeCommit PR status for {repository}#{pr_number}: {e}. '
                f'Including conversation to be safe.'
            )
            # If we can't determine the PR status, include the conversation to be safe
            return True
