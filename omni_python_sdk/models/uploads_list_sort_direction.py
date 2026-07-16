from typing import Literal

UploadsListSortDirection = Literal["asc", "desc"]

UPLOADS_LIST_SORT_DIRECTION_VALUES: set[UploadsListSortDirection] = {
    "asc",
    "desc",
}


def check_uploads_list_sort_direction(value: str) -> UploadsListSortDirection:
    if value in UPLOADS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {UPLOADS_LIST_SORT_DIRECTION_VALUES!r}")
