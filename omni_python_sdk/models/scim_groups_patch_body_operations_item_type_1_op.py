from typing import Literal

ScimGroupsPatchBodyOperationsItemType1Op = Literal["remove", "Remove"]

SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_1_OP_VALUES: set[ScimGroupsPatchBodyOperationsItemType1Op] = {
    "remove",
    "Remove",
}


def check_scim_groups_patch_body_operations_item_type_1_op(value: str) -> ScimGroupsPatchBodyOperationsItemType1Op:
    if value in SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_1_OP_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_1_OP_VALUES!r}"
    )
