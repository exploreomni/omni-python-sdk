from typing import Literal

ModelsGitUpdateResponseRequirePullRequest = Literal["always", "never", "users-only"]

MODELS_GIT_UPDATE_RESPONSE_REQUIRE_PULL_REQUEST_VALUES: set[ModelsGitUpdateResponseRequirePullRequest] = {
    "always",
    "never",
    "users-only",
}


def check_models_git_update_response_require_pull_request(value: str) -> ModelsGitUpdateResponseRequirePullRequest:
    if value in MODELS_GIT_UPDATE_RESPONSE_REQUIRE_PULL_REQUEST_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GIT_UPDATE_RESPONSE_REQUIRE_PULL_REQUEST_VALUES!r}"
    )
