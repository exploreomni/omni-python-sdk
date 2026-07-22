from typing import Literal

ModelsGitUpdateResponseAuthMethod = Literal["github_app", "https_token", "ssh"]

MODELS_GIT_UPDATE_RESPONSE_AUTH_METHOD_VALUES: set[ModelsGitUpdateResponseAuthMethod] = {
    "github_app",
    "https_token",
    "ssh",
}


def check_models_git_update_response_auth_method(value: str) -> ModelsGitUpdateResponseAuthMethod:
    if value in MODELS_GIT_UPDATE_RESPONSE_AUTH_METHOD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_GIT_UPDATE_RESPONSE_AUTH_METHOD_VALUES!r}")
