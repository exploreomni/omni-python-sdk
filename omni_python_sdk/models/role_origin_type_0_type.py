from typing import Literal

RoleOriginType0Type = Literal["USER"]

ROLE_ORIGIN_TYPE_0_TYPE_VALUES: set[RoleOriginType0Type] = {
    "USER",
}


def check_role_origin_type_0_type(value: str) -> RoleOriginType0Type:
    if value in ROLE_ORIGIN_TYPE_0_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROLE_ORIGIN_TYPE_0_TYPE_VALUES!r}")
