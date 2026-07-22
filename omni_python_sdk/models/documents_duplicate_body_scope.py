from typing import Literal

DocumentsDuplicateBodyScope = Literal["organization", "restricted"]

DOCUMENTS_DUPLICATE_BODY_SCOPE_VALUES: set[DocumentsDuplicateBodyScope] = {
    "organization",
    "restricted",
}


def check_documents_duplicate_body_scope(value: str) -> DocumentsDuplicateBodyScope:
    if value in DOCUMENTS_DUPLICATE_BODY_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_DUPLICATE_BODY_SCOPE_VALUES!r}")
