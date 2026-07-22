from typing import Literal

SuggestionRunStatus = Literal["complete", "executing", "failed", "queued"]

SUGGESTION_RUN_STATUS_VALUES: set[SuggestionRunStatus] = {
    "complete",
    "executing",
    "failed",
    "queued",
}


def check_suggestion_run_status(value: str) -> SuggestionRunStatus:
    if value in SUGGESTION_RUN_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SUGGESTION_RUN_STATUS_VALUES!r}")
