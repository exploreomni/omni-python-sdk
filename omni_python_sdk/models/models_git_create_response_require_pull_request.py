from typing import Literal

ModelsGitCreateResponseRequirePullRequest = Literal["always", "never", "users-only"]

MODELS_GIT_CREATE_RESPONSE_REQUIRE_PULL_REQUEST_VALUES: set[ModelsGitCreateResponseRequirePullRequest] = {
    "always",
    "never",
    "users-only",
}


def check_models_git_create_response_require_pull_request(value: str) -> ModelsGitCreateResponseRequirePullRequest:
    if value in MODELS_GIT_CREATE_RESPONSE_REQUIRE_PULL_REQUEST_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GIT_CREATE_RESPONSE_REQUIRE_PULL_REQUEST_VALUES!r}"
    )
