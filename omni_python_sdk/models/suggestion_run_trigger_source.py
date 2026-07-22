from typing import Literal

SuggestionRunTriggerSource = Literal["manual", "scheduled"]

SUGGESTION_RUN_TRIGGER_SOURCE_VALUES: set[SuggestionRunTriggerSource] = {
    "manual",
    "scheduled",
}


def check_suggestion_run_trigger_source(value: str) -> SuggestionRunTriggerSource:
    if value in SUGGESTION_RUN_TRIGGER_SOURCE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SUGGESTION_RUN_TRIGGER_SOURCE_VALUES!r}")
