from typing import Literal

ModelsGitCreateBodyGitServiceProvider = Literal[
    "auto", "azure_devops", "bitbucket", "bitbucket_datacenter", "github", "gitlab"
]

MODELS_GIT_CREATE_BODY_GIT_SERVICE_PROVIDER_VALUES: set[ModelsGitCreateBodyGitServiceProvider] = {
    "auto",
    "azure_devops",
    "bitbucket",
    "bitbucket_datacenter",
    "github",
    "gitlab",
}


def check_models_git_create_body_git_service_provider(value: str) -> ModelsGitCreateBodyGitServiceProvider:
    if value in MODELS_GIT_CREATE_BODY_GIT_SERVICE_PROVIDER_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GIT_CREATE_BODY_GIT_SERVICE_PROVIDER_VALUES!r}"
    )
