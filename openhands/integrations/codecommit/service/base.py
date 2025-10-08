from typing import Any

import boto3
import httpx
from botocore.exceptions import ClientError
from pydantic import SecretStr

from openhands.integrations.protocols.http_client import HTTPClient
from openhands.integrations.service_types import (
    AuthenticationError,
    BaseGitService,
    RateLimitError,
    RequestMethod,
    ResourceNotFoundError,
    UnknownException,
    User,
)


class CodeCommitMixinBase(BaseGitService, HTTPClient):
    """
    Declares common attributes and method signatures used across mixins for AWS CodeCommit.
    """

    BASE_URL: str = 'https://codecommit.{region}.amazonaws.com'
    REGION: str = 'us-east-1'  # Default region

    def __init__(
        self,
        user_id: str | None = None,
        external_auth_id: str | None = None,
        external_auth_token: SecretStr | None = None,
        token: SecretStr | None = None,
        external_token_manager: bool = False,
        base_domain: str | None = None,
        region: str | None = None,
    ) -> None:
        self.user_id = user_id
        self.external_token_manager = external_token_manager
        self.external_auth_id = external_auth_id
        self.external_auth_token = external_auth_token

        if token:
            self.token = token

        if region:
            self.REGION = region

        self.BASE_URL = self.BASE_URL.format(region=self.REGION)
        self._client = None

    @property
    def client(self):
        """Get or create the boto3 CodeCommit client."""
        if self._client is None:
            # Use AWS credentials from token if available
            if hasattr(self, 'token') and self.token:
                # Parse the token as JSON containing AWS credentials
                import json

                try:
                    creds = json.loads(self.token.get_secret_value())
                    self._client = boto3.client(
                        'codecommit',
                        region_name=self.REGION,
                        aws_access_key_id=creds.get('aws_access_key_id'),
                        aws_secret_access_key=creds.get('aws_secret_access_key'),
                        aws_session_token=creds.get('aws_session_token'),
                    )
                except Exception as e:
                    raise AuthenticationError(f'Failed to parse AWS credentials: {e}')
            else:
                # Use default AWS credentials from environment or config
                self._client = boto3.client('codecommit', region_name=self.REGION)

        return self._client

    async def _get_headers(self) -> dict:
        """Get headers for HTTP requests (not typically used with boto3)."""
        return {}

    async def get_latest_token(self) -> SecretStr | None:  # type: ignore[override]
        return self.token

    async def _make_request(
        self,
        url: str,
        params: dict | None = None,
        method: RequestMethod = RequestMethod.GET,
    ) -> tuple[Any, dict]:  # type: ignore[override]
        """
        This method is required by BaseGitService but not typically used with boto3.
        It's implemented for compatibility with the interface.
        """
        try:
            async with httpx.AsyncClient() as client:
                headers = await self._get_headers()

                response = await self.execute_request(
                    client=client,
                    url=url,
                    headers=headers,
                    params=params,
                    method=method,
                )

                response.raise_for_status()
                headers = {}

                content_type = response.headers.get('Content-Type', '')
                if 'application/json' in content_type:
                    return response.json(), headers
                else:
                    return response.text, headers

        except httpx.HTTPStatusError as e:
            raise self.handle_http_status_error(e)
        except httpx.HTTPError as e:
            raise self.handle_http_error(e)

    def handle_boto3_error(self, error: ClientError) -> Exception:
        """Handle boto3 client errors and convert to appropriate exceptions."""
        error_code = error.response.get('Error', {}).get('Code', '')

        if error_code == 'AccessDeniedException':
            return AuthenticationError(f'AWS CodeCommit authentication error: {error}')
        elif error_code == 'ResourceNotFoundException':
            return ResourceNotFoundError(f'AWS CodeCommit resource not found: {error}')
        elif error_code == 'ThrottlingException':
            return RateLimitError(f'AWS CodeCommit rate limit exceeded: {error}')
        else:
            return UnknownException(f'AWS CodeCommit error: {error}')

    async def verify_access(self) -> bool:
        """Verify access to AWS CodeCommit."""
        try:
            # List repositories to verify access
            self.client.list_repositories()
            return True
        except ClientError as e:
            raise self.handle_boto3_error(e)

    async def get_user(self) -> User:
        """Get the authenticated user's information."""
        try:
            # AWS CodeCommit doesn't have a direct equivalent to get user info
            # Use AWS STS to get caller identity
            sts_client = boto3.client('sts', region_name=self.REGION)
            identity = sts_client.get_caller_identity()

            # Extract user ID from ARN
            arn = identity['Arn']
            user_id = arn.split('/')[-1] if '/' in arn else arn.split(':')[-1]

            return User(
                id=identity['UserId'],
                login=user_id,
                avatar_url='',  # AWS doesn't provide avatar URLs
                name=user_id,
                email=None,
                company=None,
            )
        except ClientError as e:
            raise self.handle_boto3_error(e)
