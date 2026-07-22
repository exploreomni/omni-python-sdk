from typing import Literal

ModelsGitCreateResponseAuthMethod = Literal["github_app", "https_token", "ssh"]

MODELS_GIT_CREATE_RESPONSE_AUTH_METHOD_VALUES: set[ModelsGitCreateResponseAuthMethod] = {
    "github_app",
    "https_token",
    "ssh",
}


def check_models_git_create_response_auth_method(value: str) -> ModelsGitCreateResponseAuthMethod:
    if value in MODELS_GIT_CREATE_RESPONSE_AUTH_METHOD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_GIT_CREATE_RESPONSE_AUTH_METHOD_VALUES!r}")
