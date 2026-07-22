from typing import Literal

ModelsGitUpdateBodyRequirePullRequest = Literal["always", "never", "users-only"]

MODELS_GIT_UPDATE_BODY_REQUIRE_PULL_REQUEST_VALUES: set[ModelsGitUpdateBodyRequirePullRequest] = {
    "always",
    "never",
    "users-only",
}


def check_models_git_update_body_require_pull_request(value: str) -> ModelsGitUpdateBodyRequirePullRequest:
    if value in MODELS_GIT_UPDATE_BODY_REQUIRE_PULL_REQUEST_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {MODELS_GIT_UPDATE_BODY_REQUIRE_PULL_REQUEST_VALUES!r}"
    )
