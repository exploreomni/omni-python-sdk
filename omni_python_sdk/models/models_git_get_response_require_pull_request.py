from typing import Literal

ModelsGitGetResponseRequirePullRequest = Literal["always", "never", "users-only"]

MODELS_GIT_GET_RESPONSE_REQUIRE_PULL_REQUEST_VALUES: set[ModelsGitGetResponseRequirePullRequest] = {
    "always",
    "never",
    "users-only",
}


def check_models_git_get_response_require_pull_request(value: str) -> ModelsGitGetResponseRequirePullRequest:
    if value in MODELS_GIT_GET_RESPONSE_REQUIRE_PULL_REQUEST_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GIT_GET_RESPONSE_REQUIRE_PULL_REQUEST_VALUES!r}"
    )
