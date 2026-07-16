from typing import Literal

DocumentsListSortField = Literal["favorites", "name", "updatedAt", "visits"]

DOCUMENTS_LIST_SORT_FIELD_VALUES: set[DocumentsListSortField] = {
    "favorites",
    "name",
    "updatedAt",
    "visits",
}


def check_documents_list_sort_field(value: str) -> DocumentsListSortField:
    if value in DOCUMENTS_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_LIST_SORT_FIELD_VALUES!r}")
