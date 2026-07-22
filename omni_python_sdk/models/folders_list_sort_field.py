from typing import Literal

FoldersListSortField = Literal["createdAt", "favorites", "name", "path", "updatedAt"]

FOLDERS_LIST_SORT_FIELD_VALUES: set[FoldersListSortField] = {
    "createdAt",
    "favorites",
    "name",
    "path",
    "updatedAt",
}


def check_folders_list_sort_field(value: str) -> FoldersListSortField:
    if value in FOLDERS_LIST_SORT_FIELD_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {FOLDERS_LIST_SORT_FIELD_VALUES!r}")
