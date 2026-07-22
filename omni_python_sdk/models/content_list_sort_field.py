from typing import Literal

ContentListSortField = Literal["favorites", "name"]

CONTENT_LIST_SORT_FIELD_VALUES: set[ContentListSortField] = {
    "favorites",
    "name",
}


def check_content_list_sort_field(value: str) -> ContentListSortField:
    if value in CONTENT_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {CONTENT_LIST_SORT_FIELD_VALUES!r}")
