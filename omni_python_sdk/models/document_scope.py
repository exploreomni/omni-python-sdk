from typing import Literal

DocumentScope = Literal["organization", "restricted"]

DOCUMENT_SCOPE_VALUES: set[DocumentScope] = {
    "organization",
    "restricted",
}


def check_document_scope(value: str) -> DocumentScope:
    if value in DOCUMENT_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENT_SCOPE_VALUES!r}")
