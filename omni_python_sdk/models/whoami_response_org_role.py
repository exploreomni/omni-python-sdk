from typing import Literal

WhoamiResponseOrgRole = Literal["MEMBER", "ORG_ADMIN"]

WHOAMI_RESPONSE_ORG_ROLE_VALUES: set[WhoamiResponseOrgRole] = {
    "MEMBER",
    "ORG_ADMIN",
}


def check_whoami_response_org_role(value: str) -> WhoamiResponseOrgRole:
    if value in WHOAMI_RESPONSE_ORG_ROLE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {WHOAMI_RESPONSE_ORG_ROLE_VALUES!r}")
