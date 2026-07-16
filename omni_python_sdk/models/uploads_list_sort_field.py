from typing import Literal

UploadsListSortField = Literal["createdAt", "fileName", "updatedAt"]

UPLOADS_LIST_SORT_FIELD_VALUES: set[UploadsListSortField] = {
    "createdAt",
    "fileName",
    "updatedAt",
}


def check_uploads_list_sort_field(value: str) -> UploadsListSortField:
    if value in UPLOADS_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {UPLOADS_LIST_SORT_FIELD_VALUES!r}")
