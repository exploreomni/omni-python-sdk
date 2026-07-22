from typing import Literal

DocumentsAccessListType = Literal["user", "userGroup"]

DOCUMENTS_ACCESS_LIST_TYPE_VALUES: set[DocumentsAccessListType] = {
    "user",
    "userGroup",
}


def check_documents_access_list_type(value: str) -> DocumentsAccessListType:
    if value in DOCUMENTS_ACCESS_LIST_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_ACCESS_LIST_TYPE_VALUES!r}")
