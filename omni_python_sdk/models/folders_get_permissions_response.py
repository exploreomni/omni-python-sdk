from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.folders_get_permissions_response_permits_item import FoldersGetPermissionsResponsePermitsItem


T = TypeVar("T", bound="FoldersGetPermissionsResponse")


@_attrs_define
class FoldersGetPermissionsResponse:
    """
    Attributes:
        permits (list[FoldersGetPermissionsResponsePermitsItem]): List of permission permits for the folder
    """

    permits: list[FoldersGetPermissionsResponsePermitsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        permits = []
        for permits_item_data in self.permits:
            permits_item = permits_item_data.to_dict()
            permits.append(permits_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "permits": permits,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.folders_get_permissions_response_permits_item import FoldersGetPermissionsResponsePermitsItem

        d = dict(src_dict)
        permits = []
        _permits = d.pop("permits")
        for permits_item_data in _permits:
            permits_item = FoldersGetPermissionsResponsePermitsItem.from_dict(permits_item_data)

            permits.append(permits_item)

        folders_get_permissions_response = cls(
            permits=permits,
        )

        folders_get_permissions_response.additional_properties = d
        return folders_get_permissions_response

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
