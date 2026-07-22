from typing import Literal

ScimGroupsPatchBodySchemasItem = Literal["urn:ietf:params:scim:api:messages:2.0:PatchOp"]

SCIM_GROUPS_PATCH_BODY_SCHEMAS_ITEM_VALUES: set[ScimGroupsPatchBodySchemasItem] = {
    "urn:ietf:params:scim:api:messages:2.0:PatchOp",
}


def check_scim_groups_patch_body_schemas_item(value: str) -> ScimGroupsPatchBodySchemasItem:
    if value in SCIM_GROUPS_PATCH_BODY_SCHEMAS_ITEM_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_PATCH_BODY_SCHEMAS_ITEM_VALUES!r}")
