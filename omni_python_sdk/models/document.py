from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_scope import DocumentScope, check_document_scope
from ..models.document_type import DocumentType, check_document_type
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_count import DocumentCount
    from ..models.document_folder_type_0 import DocumentFolderType0
    from ..models.document_owner import DocumentOwner


T = TypeVar("T", bound="Document")


@_attrs_define
class Document:
    """
    Attributes:
        connection_id (str): Connection ID the document is associated with
        deleted (bool): Whether the document is deleted (archived)
        folder (DocumentFolderType0 | None): Folder containing the document
        has_dashboard (bool): Whether the document has an associated dashboard
        identifier (str): Document identifier Example: abc123.
        name (str): Document name
        owner (DocumentOwner): Document owner
        scope (DocumentScope): Document access scope
        type_ (DocumentType): Content type
        updated_at (datetime.datetime | None): Last updated timestamp
        url (str): URL to view the document. Returns the dashboard URL if it has a dashboard, the app URL if it has an
            app, otherwise the workbook URL. Example: https://org.omni.co/dashboards/abc123.
        field_count (DocumentCount | Unset): Document counts (included when _count is in include param)
        description (None | str | Unset): Document description
        has_app (bool | Unset): Whether the document has an associated app
        labels (list[str] | Unset): Labels applied to the document (included when labels is in include param)
    """

    connection_id: str
    deleted: bool
    folder: DocumentFolderType0 | None
    has_dashboard: bool
    identifier: str
    name: str
    owner: DocumentOwner
    scope: DocumentScope
    type_: DocumentType
    updated_at: datetime.datetime | None
    url: str
    field_count: DocumentCount | Unset = UNSET
    description: None | str | Unset = UNSET
    has_app: bool | Unset = UNSET
    labels: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.document_folder_type_0 import DocumentFolderType0

        connection_id = self.connection_id

        deleted = self.deleted

        folder: dict[str, Any] | None
        if isinstance(self.folder, DocumentFolderType0):
            folder = self.folder.to_dict()
        else:
            folder = self.folder

        has_dashboard = self.has_dashboard

        identifier = self.identifier

        name = self.name

        owner = self.owner.to_dict()

        scope: str = self.scope

        type_: str = self.type_

        updated_at: None | str
        if isinstance(self.updated_at, datetime.datetime):
            updated_at = self.updated_at.isoformat()
        else:
            updated_at = self.updated_at

        url = self.url

        field_count: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_count, Unset):
            field_count = self.field_count.to_dict()

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        has_app = self.has_app

        labels: list[str] | Unset = UNSET
        if not isinstance(self.labels, Unset):
            labels = self.labels

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connectionId": connection_id,
                "deleted": deleted,
                "folder": folder,
                "hasDashboard": has_dashboard,
                "identifier": identifier,
                "name": name,
                "owner": owner,
                "scope": scope,
                "type": type_,
                "updatedAt": updated_at,
                "url": url,
            }
        )
        if field_count is not UNSET:
            field_dict["_count"] = field_count
        if description is not UNSET:
            field_dict["description"] = description
        if has_app is not UNSET:
            field_dict["hasApp"] = has_app
        if labels is not UNSET:
            field_dict["labels"] = labels

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_count import DocumentCount
        from ..models.document_folder_type_0 import DocumentFolderType0
        from ..models.document_owner import DocumentOwner

        d = dict(src_dict)
        connection_id = d.pop("connectionId")

        deleted = d.pop("deleted")

        def _parse_folder(data: object) -> DocumentFolderType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_document_folder_type_0 = DocumentFolderType0.from_dict(data)

                return componentsschemas_document_folder_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(DocumentFolderType0 | None, data)

        folder = _parse_folder(d.pop("folder"))

        has_dashboard = d.pop("hasDashboard")

        identifier = d.pop("identifier")

        name = d.pop("name")

        owner = DocumentOwner.from_dict(d.pop("owner"))

        scope = check_document_scope(d.pop("scope"))

        type_ = check_document_type(d.pop("type"))

        def _parse_updated_at(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                updated_at_type_0 = datetime.datetime.fromisoformat(data)

                return updated_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        updated_at = _parse_updated_at(d.pop("updatedAt"))

        url = d.pop("url")

        _field_count = d.pop("_count", UNSET)
        field_count: DocumentCount | Unset
        if isinstance(_field_count, Unset):
            field_count = UNSET
        else:
            field_count = DocumentCount.from_dict(_field_count)

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        has_app = d.pop("hasApp", UNSET)

        labels = cast(list[str], d.pop("labels", UNSET))

        document = cls(
            connection_id=connection_id,
            deleted=deleted,
            folder=folder,
            has_dashboard=has_dashboard,
            identifier=identifier,
            name=name,
            owner=owner,
            scope=scope,
            type_=type_,
            updated_at=updated_at,
            url=url,
            field_count=field_count,
            description=description,
            has_app=has_app,
            labels=labels,
        )

        document.additional_properties = d
        return document

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
