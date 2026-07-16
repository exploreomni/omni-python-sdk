from typing import Literal

ModelSuggestionsListStatus = Literal["active", "all", "ignored"]

MODEL_SUGGESTIONS_LIST_STATUS_VALUES: set[ModelSuggestionsListStatus] = {
    "active",
    "all",
    "ignored",
}


def check_model_suggestions_list_status(value: str) -> ModelSuggestionsListStatus:
    if value in MODEL_SUGGESTIONS_LIST_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODEL_SUGGESTIONS_LIST_STATUS_VALUES!r}")
