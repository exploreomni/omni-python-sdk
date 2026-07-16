from typing import Literal

ContentShareScope = Literal["organization", "restricted"]

CONTENT_SHARE_SCOPE_VALUES: set[ContentShareScope] = {
    "organization",
    "restricted",
}


def check_content_share_scope(value: str) -> ContentShareScope:
    if value in CONTENT_SHARE_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {CONTENT_SHARE_SCOPE_VALUES!r}")
