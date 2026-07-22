from typing import Literal

FoldersCreateBodyScope = Literal["organization", "restricted"]

FOLDERS_CREATE_BODY_SCOPE_VALUES: set[FoldersCreateBodyScope] = {
    "organization",
    "restricted",
}


def check_folders_create_body_scope(value: str) -> FoldersCreateBodyScope:
    if value in FOLDERS_CREATE_BODY_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {FOLDERS_CREATE_BODY_SCOPE_VALUES!r}")
