from typing import Literal

ModelsGitUpdateBodyGitServiceProvider = Literal[
    "auto", "azure_devops", "bitbucket", "bitbucket_datacenter", "github", "gitlab"
]

MODELS_GIT_UPDATE_BODY_GIT_SERVICE_PROVIDER_VALUES: set[ModelsGitUpdateBodyGitServiceProvider] = {
    "auto",
    "azure_devops",
    "bitbucket",
    "bitbucket_datacenter",
    "github",
    "gitlab",
}


def check_models_git_update_body_git_service_provider(value: str) -> ModelsGitUpdateBodyGitServiceProvider:
    if value in MODELS_GIT_UPDATE_BODY_GIT_SERVICE_PROVIDER_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GIT_UPDATE_BODY_GIT_SERVICE_PROVIDER_VALUES!r}"
    )
