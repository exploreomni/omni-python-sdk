from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.content_list_response_records_item_type_0_type import (
    ContentListResponseRecordsItemType0Type,
    check_content_list_response_records_item_type_0_type,
)
from ..models.content_share_scope import ContentShareScope, check_content_share_scope
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.api_document_count import ApiDocumentCount
    from ..models.internal_folder_type_0 import InternalFolderType0
    from ..models.owner_internal import OwnerInternal


T = TypeVar("T", bound="ContentListResponseRecordsItemType0")


@_attrs_define
class ContentListResponseRecordsItemType0:
    """
    Attributes:
        name (str): Content name
        owner (OwnerInternal): Content owner
        scope (ContentShareScope): Content access scope
        connection_id (str): Connection ID
        deleted (bool): Whether document is deleted
        folder (InternalFolderType0 | None): Parent folder
        has_app (bool): Whether document has an app
        has_dashboard (bool): Whether document has a dashboard
        identifier (str): Document identifier
        updated_at (datetime.datetime | None): Last updated timestamp
        url (str): URL to view the document. Returns the dashboard URL if it has a dashboard, the app URL if it has an
            app, otherwise the workbook URL. Example: https://org.omni.co/dashboards/abc123.
        type_ (ContentListResponseRecordsItemType0Type):
        field_count (ApiDocumentCount | Unset): Document counts
        description (None | str | Unset): Document description
        labels (list[str] | Unset): Applied labels
        last_viewed_at (datetime.datetime | None | Unset): Last time the dashboard was viewed
        visits (float | None | Unset): Number of dashboard visits
    """

    name: str
    owner: OwnerInternal
    scope: ContentShareScope
    connection_id: str
    deleted: bool
    folder: InternalFolderType0 | None
    has_app: bool
    has_dashboard: bool
    identifier: str
    updated_at: datetime.datetime | None
    url: str
    type_: ContentListResponseRecordsItemType0Type
    field_count: ApiDocumentCount | Unset = UNSET
    description: None | str | Unset = UNSET
    labels: list[str] | Unset = UNSET
    last_viewed_at: datetime.datetime | None | Unset = UNSET
    visits: float | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.internal_folder_type_0 import InternalFolderType0

        name = self.name

        owner = self.owner.to_dict()

        scope: str = self.scope

        connection_id = self.connection_id

        deleted = self.deleted

        folder: dict[str, Any] | None
        if isinstance(self.folder, InternalFolderType0):
            folder = self.folder.to_dict()
        else:
            folder = self.folder

        has_app = self.has_app

        has_dashboard = self.has_dashboard

        identifier = self.identifier

        updated_at: None | str
        if isinstance(self.updated_at, datetime.datetime):
            updated_at = self.updated_at.isoformat()
        else:
            updated_at = self.updated_at

        url = self.url

        type_: str = self.type_

        field_count: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_count, Unset):
            field_count = self.field_count.to_dict()

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        labels: list[str] | Unset = UNSET
        if not isinstance(self.labels, Unset):
            labels = self.labels

        last_viewed_at: None | str | Unset
        if isinstance(self.last_viewed_at, Unset):
            last_viewed_at = UNSET
        elif isinstance(self.last_viewed_at, datetime.datetime):
            last_viewed_at = self.last_viewed_at.isoformat()
        else:
            last_viewed_at = self.last_viewed_at

        visits: float | None | Unset
        if isinstance(self.visits, Unset):
            visits = UNSET
        else:
            visits = self.visits

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "owner": owner,
                "scope": scope,
                "connectionId": connection_id,
                "deleted": deleted,
                "folder": folder,
                "hasApp": has_app,
                "hasDashboard": has_dashboard,
                "identifier": identifier,
                "updatedAt": updated_at,
                "url": url,
                "type": type_,
            }
        )
        if field_count is not UNSET:
            field_dict["_count"] = field_count
        if description is not UNSET:
            field_dict["description"] = description
        if labels is not UNSET:
            field_dict["labels"] = labels
        if last_viewed_at is not UNSET:
            field_dict["lastViewedAt"] = last_viewed_at
        if visits is not UNSET:
            field_dict["visits"] = visits

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.api_document_count import ApiDocumentCount
        from ..models.internal_folder_type_0 import InternalFolderType0
        from ..models.owner_internal import OwnerInternal

        d = dict(src_dict)
        name = d.pop("name")

        owner = OwnerInternal.from_dict(d.pop("owner"))

        scope = check_content_share_scope(d.pop("scope"))

        connection_id = d.pop("connectionId")

        deleted = d.pop("deleted")

        def _parse_folder(data: object) -> InternalFolderType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_internal_folder_type_0 = InternalFolderType0.from_dict(data)

                return componentsschemas_internal_folder_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(InternalFolderType0 | None, data)

        folder = _parse_folder(d.pop("folder"))

        has_app = d.pop("hasApp")

        has_dashboard = d.pop("hasDashboard")

        identifier = d.pop("identifier")

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

        type_ = check_content_list_response_records_item_type_0_type(d.pop("type"))

        _field_count = d.pop("_count", UNSET)
        field_count: ApiDocumentCount | Unset
        if isinstance(_field_count, Unset):
            field_count = UNSET
        else:
            field_count = ApiDocumentCount.from_dict(_field_count)

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        labels = cast(list[str], d.pop("labels", UNSET))

        def _parse_last_viewed_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                last_viewed_at_type_0 = datetime.datetime.fromisoformat(data)

                return last_viewed_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        last_viewed_at = _parse_last_viewed_at(d.pop("lastViewedAt", UNSET))

        def _parse_visits(data: object) -> float | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(float | None | Unset, data)

        visits = _parse_visits(d.pop("visits", UNSET))

        content_list_response_records_item_type_0 = cls(
            name=name,
            owner=owner,
            scope=scope,
            connection_id=connection_id,
            deleted=deleted,
            folder=folder,
            has_app=has_app,
            has_dashboard=has_dashboard,
            identifier=identifier,
            updated_at=updated_at,
            url=url,
            type_=type_,
            field_count=field_count,
            description=description,
            labels=labels,
            last_viewed_at=last_viewed_at,
            visits=visits,
        )

        content_list_response_records_item_type_0.additional_properties = d
        return content_list_response_records_item_type_0

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
