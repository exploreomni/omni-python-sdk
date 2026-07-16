from typing import Literal

ApiDraftStatus = Literal["active", "archived"]

API_DRAFT_STATUS_VALUES: set[ApiDraftStatus] = {
    "active",
    "archived",
}


def check_api_draft_status(value: str) -> ApiDraftStatus:
    if value in API_DRAFT_STATUS_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {API_DRAFT_STATUS_VALUES!r}")
