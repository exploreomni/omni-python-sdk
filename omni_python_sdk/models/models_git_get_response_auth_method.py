from typing import Literal

ModelsGitGetResponseAuthMethod = Literal["github_app", "https_token", "ssh"]

MODELS_GIT_GET_RESPONSE_AUTH_METHOD_VALUES: set[ModelsGitGetResponseAuthMethod] = {
    "github_app",
    "https_token",
    "ssh",
}


def check_models_git_get_response_auth_method(value: str) -> ModelsGitGetResponseAuthMethod:
    if value in MODELS_GIT_GET_RESPONSE_AUTH_METHOD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_GIT_GET_RESPONSE_AUTH_METHOD_VALUES!r}")
