from typing import Literal

ScimUserPatchRequestOperationsItemOp = Literal["Add", "add", "Remove", "remove", "Replace", "replace"]

SCIM_USER_PATCH_REQUEST_OPERATIONS_ITEM_OP_VALUES: set[ScimUserPatchRequestOperationsItemOp] = {
    "Add",
    "add",
    "Remove",
    "remove",
    "Replace",
    "replace",
}


def check_scim_user_patch_request_operations_item_op(value: str) -> ScimUserPatchRequestOperationsItemOp:
    if value in SCIM_USER_PATCH_REQUEST_OPERATIONS_ITEM_OP_VALUES:
        return value
    raise TypeError(
        f"Unexpected value {value!r}. Expected one of {SCIM_USER_PATCH_REQUEST_OPERATIONS_ITEM_OP_VALUES!r}"
    )
