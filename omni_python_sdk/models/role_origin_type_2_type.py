from typing import Literal

RoleOriginType2Type = Literal["BASE"]

ROLE_ORIGIN_TYPE_2_TYPE_VALUES: set[RoleOriginType2Type] = {
    "BASE",
}


def check_role_origin_type_2_type(value: str) -> RoleOriginType2Type:
    if value in ROLE_ORIGIN_TYPE_2_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROLE_ORIGIN_TYPE_2_TYPE_VALUES!r}")
