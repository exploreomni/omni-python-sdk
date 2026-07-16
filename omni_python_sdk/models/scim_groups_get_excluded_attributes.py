from typing import Literal

ScimGroupsGetExcludedAttributes = Literal["members"]

SCIM_GROUPS_GET_EXCLUDED_ATTRIBUTES_VALUES: set[ScimGroupsGetExcludedAttributes] = {
    "members",
}


def check_scim_groups_get_excluded_attributes(value: str) -> ScimGroupsGetExcludedAttributes:
    if value in SCIM_GROUPS_GET_EXCLUDED_ATTRIBUTES_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_GET_EXCLUDED_ATTRIBUTES_VALUES!r}")
