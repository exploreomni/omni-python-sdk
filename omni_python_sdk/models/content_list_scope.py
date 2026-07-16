from typing import Literal

ContentListScope = Literal["organization", "restricted"]

CONTENT_LIST_SCOPE_VALUES: set[ContentListScope] = {
    "organization",
    "restricted",
}


def check_content_list_scope(value: str) -> ContentListScope:
    if value in CONTENT_LIST_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {CONTENT_LIST_SCOPE_VALUES!r}")
