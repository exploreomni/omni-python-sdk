from typing import Literal

GenerateSuggestionsResponseStatus = Literal["queued"]

GENERATE_SUGGESTIONS_RESPONSE_STATUS_VALUES: set[GenerateSuggestionsResponseStatus] = {
    "queued",
}


def check_generate_suggestions_response_status(value: str) -> GenerateSuggestionsResponseStatus:
    if value in GENERATE_SUGGESTIONS_RESPONSE_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {GENERATE_SUGGESTIONS_RESPONSE_STATUS_VALUES!r}")
