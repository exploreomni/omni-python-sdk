from typing import Literal

ApiKeysListSortField = Literal["createdAt", "name"]

API_KEYS_LIST_SORT_FIELD_VALUES: set[ApiKeysListSortField] = {
    "createdAt",
    "name",
}


def check_api_keys_list_sort_field(value: str) -> ApiKeysListSortField:
    if value in API_KEYS_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {API_KEYS_LIST_SORT_FIELD_VALUES!r}")
