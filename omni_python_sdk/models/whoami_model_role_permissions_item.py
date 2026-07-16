from typing import Literal

WhoamiModelRolePermissionsItem = Literal[
    "DOWNLOAD_CONTENT_QUERY",
    "QUERY_FULL_MODEL",
    "QUERY_SQL",
    "QUERY_TOPICS",
    "RUN_CONTENT_QUERIES",
    "SAVE_SPREADSHEETS",
    "SCHEDULE",
    "UPDATE",
    "UPDATE_RESTRICTED",
    "UPLOAD_CSV",
    "USE_AI",
    "USE_IDE",
    "USE_WORKBOOKS",
    "VIEW_SQL",
]

WHOAMI_MODEL_ROLE_PERMISSIONS_ITEM_VALUES: set[WhoamiModelRolePermissionsItem] = {
    "DOWNLOAD_CONTENT_QUERY",
    "QUERY_FULL_MODEL",
    "QUERY_SQL",
    "QUERY_TOPICS",
    "RUN_CONTENT_QUERIES",
    "SAVE_SPREADSHEETS",
    "SCHEDULE",
    "UPDATE",
    "UPDATE_RESTRICTED",
    "UPLOAD_CSV",
    "USE_AI",
    "USE_IDE",
    "USE_WORKBOOKS",
    "VIEW_SQL",
}


def check_whoami_model_role_permissions_item(value: str) -> WhoamiModelRolePermissionsItem:
    if value in WHOAMI_MODEL_ROLE_PERMISSIONS_ITEM_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {WHOAMI_MODEL_ROLE_PERMISSIONS_ITEM_VALUES!r}")
