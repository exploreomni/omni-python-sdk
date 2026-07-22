from typing import Literal

ScimGroupsPatchBodyOperationsItemType0Op = Literal["Replace", "replace"]

SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_0_OP_VALUES: set[ScimGroupsPatchBodyOperationsItemType0Op] = {
    "Replace",
    "replace",
}


def check_scim_groups_patch_body_operations_item_type_0_op(value: str) -> ScimGroupsPatchBodyOperationsItemType0Op:
    if value in SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_0_OP_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_0_OP_VALUES!r}"
    )
