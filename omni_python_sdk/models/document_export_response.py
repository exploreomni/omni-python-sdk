from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_export_response_document import DocumentExportResponseDocument
    from ..models.document_export_response_file_uploads import DocumentExportResponseFileUploads
    from ..models.document_export_response_query_models import DocumentExportResponseQueryModels


T = TypeVar("T", bound="DocumentExportResponse")


@_attrs_define
class DocumentExportResponse:
    """
    Attributes:
        document (DocumentExportResponseDocument):
        export_version (str):
        query_models (DocumentExportResponseQueryModels):
        dashboard (Any | Unset): Dashboard configuration and layout
        file_uploads (DocumentExportResponseFileUploads | Unset):
        workbook_model (Any | Unset):
    """

    document: DocumentExportResponseDocument
    export_version: str
    query_models: DocumentExportResponseQueryModels
    dashboard: Any | Unset = UNSET
    file_uploads: DocumentExportResponseFileUploads | Unset = UNSET
    workbook_model: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        document = self.document.to_dict()

        export_version = self.export_version

        query_models = self.query_models.to_dict()

        dashboard = self.dashboard

        file_uploads: dict[str, Any] | Unset = UNSET
        if not isinstance(self.file_uploads, Unset):
            file_uploads = self.file_uploads.to_dict()

        workbook_model = self.workbook_model

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "document": document,
                "exportVersion": export_version,
                "queryModels": query_models,
            }
        )
        if dashboard is not UNSET:
            field_dict["dashboard"] = dashboard
        if file_uploads is not UNSET:
            field_dict["fileUploads"] = file_uploads
        if workbook_model is not UNSET:
            field_dict["workbookModel"] = workbook_model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_export_response_document import DocumentExportResponseDocument
        from ..models.document_export_response_file_uploads import DocumentExportResponseFileUploads
        from ..models.document_export_response_query_models import DocumentExportResponseQueryModels

        d = dict(src_dict)
        document = DocumentExportResponseDocument.from_dict(d.pop("document"))

        export_version = d.pop("exportVersion")

        query_models = DocumentExportResponseQueryModels.from_dict(d.pop("queryModels"))

        dashboard = d.pop("dashboard", UNSET)

        _file_uploads = d.pop("fileUploads", UNSET)
        file_uploads: DocumentExportResponseFileUploads | Unset
        if isinstance(_file_uploads, Unset):
            file_uploads = UNSET
        else:
            file_uploads = DocumentExportResponseFileUploads.from_dict(_file_uploads)

        workbook_model = d.pop("workbookModel", UNSET)

        document_export_response = cls(
            document=document,
            export_version=export_version,
            query_models=query_models,
            dashboard=dashboard,
            file_uploads=file_uploads,
            workbook_model=workbook_model,
        )

        document_export_response.additional_properties = d
        return document_export_response

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
