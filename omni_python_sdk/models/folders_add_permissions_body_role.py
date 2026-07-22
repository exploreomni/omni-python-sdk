from typing import Literal

FoldersAddPermissionsBodyRole = Literal["EDITOR", "EXPLORER", "MANAGER", "NO_ACCESS", "VIEWER"]

FOLDERS_ADD_PERMISSIONS_BODY_ROLE_VALUES: set[FoldersAddPermissionsBodyRole] = {
    "EDITOR",
    "EXPLORER",
    "MANAGER",
    "NO_ACCESS",
    "VIEWER",
}


def check_folders_add_permissions_body_role(value: str) -> FoldersAddPermissionsBodyRole:
    if value in FOLDERS_ADD_PERMISSIONS_BODY_ROLE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {FOLDERS_ADD_PERMISSIONS_BODY_ROLE_VALUES!r}")
