from typing import Literal

DocumentsAddPermitsBodyRole = Literal["EDITOR", "EXPLORER", "MANAGER", "NO_ACCESS", "VIEWER"]

DOCUMENTS_ADD_PERMITS_BODY_ROLE_VALUES: set[DocumentsAddPermitsBodyRole] = {
    "EDITOR",
    "EXPLORER",
    "MANAGER",
    "NO_ACCESS",
    "VIEWER",
}


def check_documents_add_permits_body_role(value: str) -> DocumentsAddPermitsBodyRole:
    if value in DOCUMENTS_ADD_PERMITS_BODY_ROLE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_ADD_PERMITS_BODY_ROLE_VALUES!r}")
