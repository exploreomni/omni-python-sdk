from typing import Literal

UploadsListType = Literal["csv", "spreadsheet"]

UPLOADS_LIST_TYPE_VALUES: set[UploadsListType] = {
    "csv",
    "spreadsheet",
}


def check_uploads_list_type(value: str) -> UploadsListType:
    if value in UPLOADS_LIST_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {UPLOADS_LIST_TYPE_VALUES!r}")
