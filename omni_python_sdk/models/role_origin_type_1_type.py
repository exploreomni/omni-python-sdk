from typing import Literal

RoleOriginType1Type = Literal["ORG"]

ROLE_ORIGIN_TYPE_1_TYPE_VALUES: set[RoleOriginType1Type] = {
    "ORG",
}


def check_role_origin_type_1_type(value: str) -> RoleOriginType1Type:
    if value in ROLE_ORIGIN_TYPE_1_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {ROLE_ORIGIN_TYPE_1_TYPE_VALUES!r}")
