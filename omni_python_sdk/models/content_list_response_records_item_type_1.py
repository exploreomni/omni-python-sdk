from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.content_list_response_records_item_type_1_scope import (
    ContentListResponseRecordsItemType1Scope,
    check_content_list_response_records_item_type_1_scope,
)
from ..models.content_list_response_records_item_type_1_type import (
    ContentListResponseRecordsItemType1Type,
    check_content_list_response_records_item_type_1_type,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.content_list_response_records_item_type_1_count import ContentListResponseRecordsItemType1Count
    from ..models.content_list_response_records_item_type_1_owner import ContentListResponseRecordsItemType1Owner


T = TypeVar("T", bound="ContentListResponseRecordsItemType1")


@_attrs_define
class ContentListResponseRecordsItemType1:
    """
    Attributes:
        id (str): Unique identifier
        name (str): Content name
        owner (ContentListResponseRecordsItemType1Owner): Content owner
        scope (ContentListResponseRecordsItemType1Scope): Content access scope
        path (str): Full path to the folder Example: sales-reports/q1-2026.
        url (str): URL to view the folder in the Omni UI. Example: https://org.omni.co/f/sales-reports.
        type_ (ContentListResponseRecordsItemType1Type):
        field_count (ContentListResponseRecordsItemType1Count | Unset): Folder counts
        labels (list[str] | Unset): Labels
    """

    id: str
    name: str
    owner: ContentListResponseRecordsItemType1Owner
    scope: ContentListResponseRecordsItemType1Scope
    path: str
    url: str
    type_: ContentListResponseRecordsItemType1Type
    field_count: ContentListResponseRecordsItemType1Count | Unset = UNSET
    labels: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        owner = self.owner.to_dict()

        scope: str = self.scope

        path = self.path

        url = self.url

        type_: str = self.type_

        field_count: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_count, Unset):
            field_count = self.field_count.to_dict()

        labels: list[str] | Unset = UNSET
        if not isinstance(self.labels, Unset):
            labels = self.labels

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "owner": owner,
                "scope": scope,
                "path": path,
                "url": url,
                "type": type_,
            }
        )
        if field_count is not UNSET:
            field_dict["_count"] = field_count
        if labels is not UNSET:
            field_dict["labels"] = labels

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.content_list_response_records_item_type_1_count import ContentListResponseRecordsItemType1Count
        from ..models.content_list_response_records_item_type_1_owner import ContentListResponseRecordsItemType1Owner

        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        owner = ContentListResponseRecordsItemType1Owner.from_dict(d.pop("owner"))

        scope = check_content_list_response_records_item_type_1_scope(d.pop("scope"))

        path = d.pop("path")

        url = d.pop("url")

        type_ = check_content_list_response_records_item_type_1_type(d.pop("type"))

        _field_count = d.pop("_count", UNSET)
        field_count: ContentListResponseRecordsItemType1Count | Unset
        if isinstance(_field_count, Unset):
            field_count = UNSET
        else:
            field_count = ContentListResponseRecordsItemType1Count.from_dict(_field_count)

        labels = cast(list[str], d.pop("labels", UNSET))

        content_list_response_records_item_type_1 = cls(
            id=id,
            name=name,
            owner=owner,
            scope=scope,
            path=path,
            url=url,
            type_=type_,
            field_count=field_count,
            labels=labels,
        )

        content_list_response_records_item_type_1.additional_properties = d
        return content_list_response_records_item_type_1

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
