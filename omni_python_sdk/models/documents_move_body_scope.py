from typing import Literal

DocumentsMoveBodyScope = Literal["organization", "restricted"]

DOCUMENTS_MOVE_BODY_SCOPE_VALUES: set[DocumentsMoveBodyScope] = {
    "organization",
    "restricted",
}


def check_documents_move_body_scope(value: str) -> DocumentsMoveBodyScope:
    if value in DOCUMENTS_MOVE_BODY_SCOPE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_MOVE_BODY_SCOPE_VALUES!r}")
