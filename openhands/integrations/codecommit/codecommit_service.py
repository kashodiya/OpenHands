import os

from pydantic import SecretStr

from openhands.integrations.codecommit.service import (
    CodeCommitBranchesMixin,
    CodeCommitFeaturesMixin,
    CodeCommitPRsMixin,
    CodeCommitReposMixin,
    CodeCommitResolverMixin,
)
from openhands.integrations.service_types import (
    BaseGitService,
    GitService,
    ProviderType,
)
from openhands.utils.import_utils import get_impl


class CodeCommitService(
    CodeCommitBranchesMixin,
    CodeCommitFeaturesMixin,
    CodeCommitPRsMixin,
    CodeCommitReposMixin,
    CodeCommitResolverMixin,
    BaseGitService,
    GitService,
):
    """
    Assembled AWS CodeCommit service class combining mixins by feature area.

    This is an extension point in OpenHands that allows applications to customize AWS CodeCommit
    integration behavior. Applications can substitute their own implementation by:
    1. Creating a class that inherits from GitService
    2. Implementing all required methods
    3. Setting server_config.codecommit_service_class to the fully qualified name of the class

    The class is instantiated via get_impl() in openhands.server.shared.py.
    """

    BASE_URL = 'https://codecommit.{region}.amazonaws.com'
    REGION = 'us-east-1'  # Default region

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

        # For AWS CodeCommit, base_domain is used to specify the region
        if base_domain:
            self.REGION = base_domain
        elif region:
            self.REGION = region

        self.BASE_URL = self.BASE_URL.format(region=self.REGION)
        self._client = None

    @property
    def provider(self) -> str:
        return ProviderType.CODECOMMIT.value


codecommit_service_cls = os.environ.get(
    'OPENHANDS_CODECOMMIT_SERVICE_CLS',
    'openhands.integrations.codecommit.codecommit_service.CodeCommitService',
)
CodeCommitServiceImpl = get_impl(CodeCommitService, codecommit_service_cls)
