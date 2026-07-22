from typing import Literal

ModelsGitCreateBodyRequirePullRequest = Literal["always", "never", "users-only"]

MODELS_GIT_CREATE_BODY_REQUIRE_PULL_REQUEST_VALUES: set[ModelsGitCreateBodyRequirePullRequest] = {
    "always",
    "never",
    "users-only",
}


def check_models_git_create_body_require_pull_request(value: str) -> ModelsGitCreateBodyRequirePullRequest:
    if value in MODELS_GIT_CREATE_BODY_REQUIRE_PULL_REQUEST_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GIT_CREATE_BODY_REQUIRE_PULL_REQUEST_VALUES!r}"
    )
