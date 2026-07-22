from typing import Literal

ModelsGitUpdateBodyAuthMethod = Literal["https_token", "ssh"]

MODELS_GIT_UPDATE_BODY_AUTH_METHOD_VALUES: set[ModelsGitUpdateBodyAuthMethod] = {
    "https_token",
    "ssh",
}


def check_models_git_update_body_auth_method(value: str) -> ModelsGitUpdateBodyAuthMethod:
    if value in MODELS_GIT_UPDATE_BODY_AUTH_METHOD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_GIT_UPDATE_BODY_AUTH_METHOD_VALUES!r}")
