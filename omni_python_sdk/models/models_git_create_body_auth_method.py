from typing import Literal

ModelsGitCreateBodyAuthMethod = Literal["https_token", "ssh"]

MODELS_GIT_CREATE_BODY_AUTH_METHOD_VALUES: set[ModelsGitCreateBodyAuthMethod] = {
    "https_token",
    "ssh",
}


def check_models_git_create_body_auth_method(value: str) -> ModelsGitCreateBodyAuthMethod:
    if value in MODELS_GIT_CREATE_BODY_AUTH_METHOD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_GIT_CREATE_BODY_AUTH_METHOD_VALUES!r}")
