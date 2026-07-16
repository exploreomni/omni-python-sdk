from typing import Literal

FoldersListScope = Literal["organization", "restricted"]

FOLDERS_LIST_SCOPE_VALUES: set[FoldersListScope] = {
    "organization",
    "restricted",
}


def check_folders_list_scope(value: str) -> FoldersListScope:
    if value in FOLDERS_LIST_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {FOLDERS_LIST_SCOPE_VALUES!r}")
