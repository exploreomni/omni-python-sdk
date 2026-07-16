from typing import Literal

RoleOriginType3Type = Literal["GROUP"]

ROLE_ORIGIN_TYPE_3_TYPE_VALUES: set[RoleOriginType3Type] = {
    "GROUP",
}


def check_role_origin_type_3_type(value: str) -> RoleOriginType3Type:
    if value in ROLE_ORIGIN_TYPE_3_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROLE_ORIGIN_TYPE_3_TYPE_VALUES!r}")
