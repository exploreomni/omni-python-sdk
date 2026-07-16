from typing import Literal

ContentFilterMode = Literal["ALL", "NO_ISSUES", "WITH_ISSUES"]

CONTENT_FILTER_MODE_VALUES: set[ContentFilterMode] = {
    "ALL",
    "NO_ISSUES",
    "WITH_ISSUES",
}


def check_content_filter_mode(value: str) -> ContentFilterMode:
    if value in CONTENT_FILTER_MODE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {CONTENT_FILTER_MODE_VALUES!r}")
