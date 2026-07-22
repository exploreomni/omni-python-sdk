from typing import Literal

ContentListSortDirection = Literal["asc", "desc"]

CONTENT_LIST_SORT_DIRECTION_VALUES: set[ContentListSortDirection] = {
    "asc",
    "desc",
}


def check_content_list_sort_direction(value: str) -> ContentListSortDirection:
    if value in CONTENT_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {CONTENT_LIST_SORT_DIRECTION_VALUES!r}")
