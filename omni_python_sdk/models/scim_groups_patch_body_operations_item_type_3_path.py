from typing import Literal

ScimGroupsPatchBodyOperationsItemType3Path = Literal["displayName", "members"]

SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_3_PATH_VALUES: set[ScimGroupsPatchBodyOperationsItemType3Path] = {
    "displayName",
    "members",
}


def check_scim_groups_patch_body_operations_item_type_3_path(value: str) -> ScimGroupsPatchBodyOperationsItemType3Path:
    if value in SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_3_PATH_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_3_PATH_VALUES!r}"
    )
