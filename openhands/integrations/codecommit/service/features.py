from botocore.exceptions import ClientError

from openhands.core.logger import openhands_logger as logger
from openhands.integrations.codecommit.service.base import CodeCommitMixinBase
from openhands.integrations.service_types import ProviderType, SuggestedTask, TaskType


class CodeCommitFeaturesMixin(CodeCommitMixinBase):
    """
    Methods for interacting with AWS CodeCommit features
    """

    async def get_suggested_tasks(self) -> list[SuggestedTask]:
        """Get suggested tasks for the authenticated user.

        Returns:
            List of suggested tasks
        """
        try:
            suggested_tasks = []

            # List all repositories
            repositories_response = self.client.list_repositories()
            repositories = repositories_response.get('repositories', [])

            for repo in repositories:
                repo_name = repo.get('repositoryName')

                # Get open pull requests
                try:
                    prs_response = self.client.list_pull_requests(
                        repositoryName=repo_name, pullRequestStatus='OPEN'
                    )

                    pr_ids = prs_response.get('pullRequestIds', [])

                    for pr_id in pr_ids:
                        try:
                            # Get pull request details
                            pr_response = self.client.get_pull_request(
                                pullRequestId=pr_id
                            )

                            pr = pr_response.get('pullRequest', {})
                            title = pr.get('title', '')

                            # Check for merge conflicts
                            if not pr.get('mergeMetadata', {}).get('isMerged', False):
                                try:
                                    # Check if PR can be merged
                                    merge_response = (
                                        self.client.test_repository_triggers(
                                            repositoryName=repo_name,
                                            triggers=[
                                                {
                                                    'destinationReference': pr.get(
                                                        'pullRequestTargets', [{}]
                                                    )[0].get(
                                                        'destinationReference', ''
                                                    ),
                                                    'sourceReference': pr.get(
                                                        'pullRequestTargets', [{}]
                                                    )[0].get('sourceReference', ''),
                                                    'name': 'test-merge',
                                                    'events': ['UPDATE_REFERENCE'],
                                                }
                                            ],
                                        )
                                    )

                                    # If there's a failure response, it might indicate merge conflicts
                                    if not merge_response.get(
                                        'successfulExecutions', []
                                    ):
                                        suggested_tasks.append(
                                            SuggestedTask(
                                                git_provider=ProviderType.CODECOMMIT,
                                                task_type=TaskType.MERGE_CONFLICTS,
                                                repo=repo_name,
                                                issue_number=int(pr_id),
                                                title=title,
                                            )
                                        )
                                except ClientError:
                                    # If we can't test the merge, assume there might be conflicts
                                    pass

                            # Check for failing checks
                            # AWS CodeCommit doesn't have built-in CI checks, but we can check approval rules
                            approval_response = (
                                self.client.evaluate_pull_request_approval_rules(
                                    pullRequestId=pr_id
                                )
                            )

                            if not approval_response.get('evaluation', {}).get(
                                'approved', True
                            ):
                                suggested_tasks.append(
                                    SuggestedTask(
                                        git_provider=ProviderType.CODECOMMIT,
                                        task_type=TaskType.FAILING_CHECKS,
                                        repo=repo_name,
                                        issue_number=int(pr_id),
                                        title=title,
                                    )
                                )

                            # Check for unresolved comments
                            comments_response = (
                                self.client.get_comments_for_pull_request(
                                    pullRequestId=pr_id
                                )
                            )

                            # Check if there are comments that might need resolution
                            if comments_response.get('commentsForPullRequestData', []):
                                suggested_tasks.append(
                                    SuggestedTask(
                                        git_provider=ProviderType.CODECOMMIT,
                                        task_type=TaskType.UNRESOLVED_COMMENTS,
                                        repo=repo_name,
                                        issue_number=int(pr_id),
                                        title=title,
                                    )
                                )

                        except ClientError as e:
                            logger.warning(f'Error getting details for PR {pr_id}: {e}')

                except ClientError as e:
                    logger.warning(
                        f'Error listing pull requests for repository {repo_name}: {e}'
                    )

            return suggested_tasks

        except ClientError as e:
            raise self.handle_boto3_error(e)

    @property
    def provider(self) -> str:
        return 'codecommit'
