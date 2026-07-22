from typing import Literal

DocumentsAccessListAccessSource = Literal["direct", "folder"]

DOCUMENTS_ACCESS_LIST_ACCESS_SOURCE_VALUES: set[DocumentsAccessListAccessSource] = {
    "direct",
    "folder",
}


def check_documents_access_list_access_source(value: str) -> DocumentsAccessListAccessSource:
    if value in DOCUMENTS_ACCESS_LIST_ACCESS_SOURCE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_ACCESS_LIST_ACCESS_SOURCE_VALUES!r}")
