from typing import Literal

ScimGroupsListExcludedAttributes = Literal["members"]

SCIM_GROUPS_LIST_EXCLUDED_ATTRIBUTES_VALUES: set[ScimGroupsListExcludedAttributes] = {
    "members",
}


def check_scim_groups_list_excluded_attributes(value: str) -> ScimGroupsListExcludedAttributes:
    if value in SCIM_GROUPS_LIST_EXCLUDED_ATTRIBUTES_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {SCIM_GROUPS_LIST_EXCLUDED_ATTRIBUTES_VALUES!r}")
