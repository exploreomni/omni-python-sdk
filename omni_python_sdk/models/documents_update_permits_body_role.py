from typing import Literal

DocumentsUpdatePermitsBodyRole = Literal["EDITOR", "EXPLORER", "MANAGER", "NO_ACCESS", "VIEWER"]

DOCUMENTS_UPDATE_PERMITS_BODY_ROLE_VALUES: set[DocumentsUpdatePermitsBodyRole] = {
    "EDITOR",
    "EXPLORER",
    "MANAGER",
    "NO_ACCESS",
    "VIEWER",
}


def check_documents_update_permits_body_role(value: str) -> DocumentsUpdatePermitsBodyRole:
    if value in DOCUMENTS_UPDATE_PERMITS_BODY_ROLE_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_UPDATE_PERMITS_BODY_ROLE_VALUES!r}")
