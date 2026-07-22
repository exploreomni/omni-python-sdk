from typing import Literal

FoldersCreateResponseScope = Literal["organization", "restricted"]

FOLDERS_CREATE_RESPONSE_SCOPE_VALUES: set[FoldersCreateResponseScope] = {
    "organization",
    "restricted",
}


def check_folders_create_response_scope(value: str) -> FoldersCreateResponseScope:
    if value in FOLDERS_CREATE_RESPONSE_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {FOLDERS_CREATE_RESPONSE_SCOPE_VALUES!r}")
