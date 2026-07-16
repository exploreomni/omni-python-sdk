from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.folders_list_response_records_item_count import FoldersListResponseRecordsItemCount


T = TypeVar("T", bound="FoldersListResponseRecordsItem")


@_attrs_define
class FoldersListResponseRecordsItem:
    """
    Attributes:
        id (UUID): Unique folder identifier
        name (str): Name of the folder Example: My Reports.
        owner_id (UUID): User ID of the folder owner
        path (str): Full path to the folder Example: /shared/reports/my-reports.
        url (str): URL to view the folder in the Omni UI. Example: https://org.omni.co/f/my-reports.
        field_count (FoldersListResponseRecordsItemCount | Unset): Count statistics for the folder
        labels (list[str] | Unset): Labels associated with the folder
    """

    id: UUID
    name: str
    owner_id: UUID
    path: str
    url: str
    field_count: FoldersListResponseRecordsItemCount | Unset = UNSET
    labels: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        name = self.name

        owner_id = str(self.owner_id)

        path = self.path

        url = self.url

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
                "ownerId": owner_id,
                "path": path,
                "url": url,
            }
        )
        if field_count is not UNSET:
            field_dict["_count"] = field_count
        if labels is not UNSET:
            field_dict["labels"] = labels

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.folders_list_response_records_item_count import FoldersListResponseRecordsItemCount

        d = dict(src_dict)
        id = UUID(d.pop("id"))

        name = d.pop("name")

        owner_id = UUID(d.pop("ownerId"))

        path = d.pop("path")

        url = d.pop("url")

        _field_count = d.pop("_count", UNSET)
        field_count: FoldersListResponseRecordsItemCount | Unset
        if isinstance(_field_count, Unset):
            field_count = UNSET
        else:
            field_count = FoldersListResponseRecordsItemCount.from_dict(_field_count)

        labels = cast(list[str], d.pop("labels", UNSET))

        folders_list_response_records_item = cls(
            id=id,
            name=name,
            owner_id=owner_id,
            path=path,
            url=url,
            field_count=field_count,
            labels=labels,
        )

        folders_list_response_records_item.additional_properties = d
        return folders_list_response_records_item

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
