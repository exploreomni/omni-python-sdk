from typing import Literal

FoldersUpdatePermissionsBodyRole = Literal["EDITOR", "EXPLORER", "MANAGER", "NO_ACCESS", "VIEWER"]

FOLDERS_UPDATE_PERMISSIONS_BODY_ROLE_VALUES: set[FoldersUpdatePermissionsBodyRole] = {
    "EDITOR",
    "EXPLORER",
    "MANAGER",
    "NO_ACCESS",
    "VIEWER",
}


def check_folders_update_permissions_body_role(value: str) -> FoldersUpdatePermissionsBodyRole:
    if value in FOLDERS_UPDATE_PERMISSIONS_BODY_ROLE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {FOLDERS_UPDATE_PERMISSIONS_BODY_ROLE_VALUES!r}")
