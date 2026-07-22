from typing import Literal

DocumentFolderType0Scope = Literal["organization", "restricted"]

DOCUMENT_FOLDER_TYPE_0_SCOPE_VALUES: set[DocumentFolderType0Scope] = {
    "organization",
    "restricted",
}


def check_document_folder_type_0_scope(value: str) -> DocumentFolderType0Scope:
    if value in DOCUMENT_FOLDER_TYPE_0_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENT_FOLDER_TYPE_0_SCOPE_VALUES!r}")
