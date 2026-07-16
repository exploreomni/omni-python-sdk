from typing import Literal

DocumentsAccessListSortDirection = Literal["asc", "desc"]

DOCUMENTS_ACCESS_LIST_SORT_DIRECTION_VALUES: set[DocumentsAccessListSortDirection] = {
    "asc",
    "desc",
}


def check_documents_access_list_sort_direction(value: str) -> DocumentsAccessListSortDirection:
    if value in DOCUMENTS_ACCESS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_ACCESS_LIST_SORT_DIRECTION_VALUES!r}")
