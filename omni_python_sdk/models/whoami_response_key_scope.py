from typing import Literal

WhoamiResponseKeyScope = Literal["organization", "user"]

WHOAMI_RESPONSE_KEY_SCOPE_VALUES: set[WhoamiResponseKeyScope] = {
    "organization",
    "user",
}


def check_whoami_response_key_scope(value: str) -> WhoamiResponseKeyScope:
    if value in WHOAMI_RESPONSE_KEY_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {WHOAMI_RESPONSE_KEY_SCOPE_VALUES!r}")
