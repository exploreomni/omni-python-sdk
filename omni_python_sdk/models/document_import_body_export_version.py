from typing import Literal

DocumentImportBodyExportVersion = Literal["0.1"]

DOCUMENT_IMPORT_BODY_EXPORT_VERSION_VALUES: set[DocumentImportBodyExportVersion] = {
    "0.1",
}


def check_document_import_body_export_version(value: str) -> DocumentImportBodyExportVersion:
    if value in DOCUMENT_IMPORT_BODY_EXPORT_VERSION_VALUES:
        return value
    raise TypeError(f"Unexpected value {value!r}. Expected one of {DOCUMENT_IMPORT_BODY_EXPORT_VERSION_VALUES!r}")
