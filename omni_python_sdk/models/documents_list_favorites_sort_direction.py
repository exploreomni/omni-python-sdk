from typing import Literal

DocumentsListFavoritesSortDirection = Literal["asc", "desc"]

DOCUMENTS_LIST_FAVORITES_SORT_DIRECTION_VALUES: set[DocumentsListFavoritesSortDirection] = {
    "asc",
    "desc",
}


def check_documents_list_favorites_sort_direction(value: str) -> DocumentsListFavoritesSortDirection:
    if value in DOCUMENTS_LIST_FAVORITES_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_LIST_FAVORITES_SORT_DIRECTION_VALUES!r}")
