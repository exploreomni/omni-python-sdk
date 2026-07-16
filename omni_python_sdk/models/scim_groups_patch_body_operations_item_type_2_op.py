from typing import Literal

ScimGroupsPatchBodyOperationsItemType2Op = Literal["add", "Add"]

SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_2_OP_VALUES: set[ScimGroupsPatchBodyOperationsItemType2Op] = {
    "add",
    "Add",
}


def check_scim_groups_patch_body_operations_item_type_2_op(value: str) -> ScimGroupsPatchBodyOperationsItemType2Op:
    if value in SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_2_OP_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_PATCH_BODY_OPERATIONS_ITEM_TYPE_2_OP_VALUES!r}"
    )
