from typing import Literal

UserGroupRoleOriginType = Literal["GROUP"]

USER_GROUP_ROLE_ORIGIN_TYPE_VALUES: set[UserGroupRoleOriginType] = {
    "GROUP",
}


def check_user_group_role_origin_type(value: str) -> UserGroupRoleOriginType:
    if value in USER_GROUP_ROLE_ORIGIN_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {USER_GROUP_ROLE_ORIGIN_TYPE_VALUES!r}")
