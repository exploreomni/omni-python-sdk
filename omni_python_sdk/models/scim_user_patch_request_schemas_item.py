from typing import Literal

ScimUserPatchRequestSchemasItem = Literal["urn:ietf:params:scim:api:messages:2.0:PatchOp"]

SCIM_USER_PATCH_REQUEST_SCHEMAS_ITEM_VALUES: set[ScimUserPatchRequestSchemasItem] = {
    "urn:ietf:params:scim:api:messages:2.0:PatchOp",
}


def check_scim_user_patch_request_schemas_item(value: str) -> ScimUserPatchRequestSchemasItem:
    if value in SCIM_USER_PATCH_REQUEST_SCHEMAS_ITEM_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCIM_USER_PATCH_REQUEST_SCHEMAS_ITEM_VALUES!r}")
