from typing import Literal

DocumentsV2GetDraftPretty = Literal["0", "1", "false", "true"]

DOCUMENTS_V2_GET_DRAFT_PRETTY_VALUES: set[DocumentsV2GetDraftPretty] = {
    "0",
    "1",
    "false",
    "true",
}


def check_documents_v2_get_draft_pretty(value: str) -> DocumentsV2GetDraftPretty:
    if value in DOCUMENTS_V2_GET_DRAFT_PRETTY_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_V2_GET_DRAFT_PRETTY_VALUES!r}")
