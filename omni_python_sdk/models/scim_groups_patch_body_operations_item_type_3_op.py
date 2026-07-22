from typing import Literal

ScimGroupsPatchBodyOperationsItemType3Op = Literal["Replace", "replace"]

SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_3_OP_VALUES: set[ScimGroupsPatchBodyOperationsItemType3Op] = {
    "Replace",
    "replace",
}


def check_scim_groups_patch_body_operations_item_type_3_op(value: str) -> ScimGroupsPatchBodyOperationsItemType3Op:
    if value in SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_3_OP_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_3_OP_VALUES!r}"
    )
