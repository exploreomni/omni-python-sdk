from typing import Literal

ModelsListIncludeDeleted = Literal["0", "1", "false", "true"]

MODELS_LIST_INCLUDE_DELETED_VALUES: set[ModelsListIncludeDeleted] = {
    "0",
    "1",
    "false",
    "true",
}


def check_models_list_include_deleted(value: str) -> ModelsListIncludeDeleted:
    if value in MODELS_LIST_INCLUDE_DELETED_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_LIST_INCLUDE_DELETED_VALUES!r}")
