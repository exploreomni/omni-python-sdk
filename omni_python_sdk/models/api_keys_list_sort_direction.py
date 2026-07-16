from typing import Literal

ApiKeysListSortDirection = Literal["asc", "desc"]

API_KEYS_LIST_SORT_DIRECTION_VALUES: set[ApiKeysListSortDirection] = {
    "asc",
    "desc",
}


def check_api_keys_list_sort_direction(value: str) -> ApiKeysListSortDirection:
    if value in API_KEYS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {API_KEYS_LIST_SORT_DIRECTION_VALUES!r}")
