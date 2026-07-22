from typing import Literal

ScheduleSuggestionsResponseStatus = Literal["enabled"]

SCHEDULE_SUGGESTIONS_RESPONSE_STATUS_VALUES: set[ScheduleSuggestionsResponseStatus] = {
    "enabled",
}


def check_schedule_suggestions_response_status(value: str) -> ScheduleSuggestionsResponseStatus:
    if value in SCHEDULE_SUGGESTIONS_RESPONSE_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCHEDULE_SUGGESTIONS_RESPONSE_STATUS_VALUES!r}")
