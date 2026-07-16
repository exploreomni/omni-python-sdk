from typing import Literal

DocumentsListSortDirection = Literal["asc", "desc"]

DOCUMENTS_LIST_SORT_DIRECTION_VALUES: set[DocumentsListSortDirection] = {
    "asc",
    "desc",
}


def check_documents_list_sort_direction(value: str) -> DocumentsListSortDirection:
    if value in DOCUMENTS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_LIST_SORT_DIRECTION_VALUES!r}")
