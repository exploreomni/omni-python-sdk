from typing import Literal

FoldersListSortDirection = Literal["asc", "desc"]

FOLDERS_LIST_SORT_DIRECTION_VALUES: set[FoldersListSortDirection] = {
    "asc",
    "desc",
}


def check_folders_list_sort_direction(value: str) -> FoldersListSortDirection:
    if value in FOLDERS_LIST_SORT_DIRECTION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {FOLDERS_LIST_SORT_DIRECTION_VALUES!r}")
