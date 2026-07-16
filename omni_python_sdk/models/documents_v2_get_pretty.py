from typing import Literal

DocumentsV2GetPretty = Literal["0", "1", "false", "true"]

DOCUMENTS_V2_GET_PRETTY_VALUES: set[DocumentsV2GetPretty] = {
    "0",
    "1",
    "false",
    "true",
}


def check_documents_v2_get_pretty(value: str) -> DocumentsV2GetPretty:
    if value in DOCUMENTS_V2_GET_PRETTY_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENTS_V2_GET_PRETTY_VALUES!r}")
