from typing import Literal

DocumentType = Literal["document"]

DOCUMENT_TYPE_VALUES: set[DocumentType] = {
    "document",
}


def check_document_type(value: str) -> DocumentType:
    if value in DOCUMENT_TYPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENT_TYPE_VALUES!r}")
