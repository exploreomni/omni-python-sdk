from typing import Literal

ModelsListSortDirection = Literal["asc", "desc"]

MODELS_LIST_SORT_DIRECTION_VALUES: set[ModelsListSortDirection] = {
    "asc",
    "desc",
}


def check_models_list_sort_direction(value: str) -> ModelsListSortDirection:
    if value in MODELS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {MODELS_LIST_SORT_DIRECTION_VALUES!r}")
