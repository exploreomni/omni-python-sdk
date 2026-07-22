from typing import Literal

ScimGroupsPatchBodyOperationsItemType2Path = Literal["members"]

SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_2_PATH_VALUES: set[ScimGroupsPatchBodyOperationsItemType2Path] = {
    "members",
}


def check_scim_groups_patch_body_operations_item_type_2_path(value: str) -> ScimGroupsPatchBodyOperationsItemType2Path:
    if value in SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_2_PATH_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_2_PATH_VALUES!r}"
    )
