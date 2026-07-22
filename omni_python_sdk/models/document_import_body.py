from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_import_body_export_version import (
    DocumentImportBodyExportVersion,
    check_document_import_body_export_version,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_import_body_document import DocumentImportBodyDocument
    from ..models.document_import_body_file_uploads import DocumentImportBodyFileUploads
    from ..models.document_import_body_query_models import DocumentImportBodyQueryModels


T = TypeVar("T", bound="DocumentImportBody")


@_attrs_define
class DocumentImportBody:
    """
    Attributes:
        base_model_id (UUID): Base model ID for the imported document
        document (DocumentImportBodyDocument):
        export_version (DocumentImportBodyExportVersion):
        query_models (DocumentImportBodyQueryModels):
        dashboard (Any | Unset): Dashboard export data
        file_uploads (DocumentImportBodyFileUploads | Unset):
        folder_path (str | Unset):
        identifier (str | Unset):
        workbook_model (Any | Unset):
    """

    base_model_id: UUID
    document: DocumentImportBodyDocument
    export_version: DocumentImportBodyExportVersion
    query_models: DocumentImportBodyQueryModels
    dashboard: Any | Unset = UNSET
    file_uploads: DocumentImportBodyFileUploads | Unset = UNSET
    folder_path: str | Unset = UNSET
    identifier: str | Unset = UNSET
    workbook_model: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base_model_id = str(self.base_model_id)

        document = self.document.to_dict()

        export_version: str = self.export_version

        query_models = self.query_models.to_dict()

        dashboard = self.dashboard

        file_uploads: dict[str, Any] | Unset = UNSET
        if not isinstance(self.file_uploads, Unset):
            file_uploads = self.file_uploads.to_dict()

        folder_path = self.folder_path

        identifier = self.identifier

        workbook_model = self.workbook_model

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "baseModelId": base_model_id,
                "document": document,
                "exportVersion": export_version,
                "queryModels": query_models,
            }
        )
        if dashboard is not UNSET:
            field_dict["dashboard"] = dashboard
        if file_uploads is not UNSET:
            field_dict["fileUploads"] = file_uploads
        if folder_path is not UNSET:
            field_dict["folderPath"] = folder_path
        if identifier is not UNSET:
            field_dict["identifier"] = identifier
        if workbook_model is not UNSET:
            field_dict["workbookModel"] = workbook_model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_import_body_document import DocumentImportBodyDocument
        from ..models.document_import_body_file_uploads import DocumentImportBodyFileUploads
        from ..models.document_import_body_query_models import DocumentImportBodyQueryModels

        d = dict(src_dict)
        base_model_id = UUID(d.pop("baseModelId"))

        document = DocumentImportBodyDocument.from_dict(d.pop("document"))

        export_version = check_document_import_body_export_version(d.pop("exportVersion"))

        query_models = DocumentImportBodyQueryModels.from_dict(d.pop("queryModels"))

        dashboard = d.pop("dashboard", UNSET)

        _file_uploads = d.pop("fileUploads", UNSET)
        file_uploads: DocumentImportBodyFileUploads | Unset
        if isinstance(_file_uploads, Unset):
            file_uploads = UNSET
        else:
            file_uploads = DocumentImportBodyFileUploads.from_dict(_file_uploads)

        folder_path = d.pop("folderPath", UNSET)

        identifier = d.pop("identifier", UNSET)

        workbook_model = d.pop("workbookModel", UNSET)

        document_import_body = cls(
            base_model_id=base_model_id,
            document=document,
            export_version=export_version,
            query_models=query_models,
            dashboard=dashboard,
            file_uploads=file_uploads,
            folder_path=folder_path,
            identifier=identifier,
            workbook_model=workbook_model,
        )

        document_import_body.additional_properties = d
        return document_import_body

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
