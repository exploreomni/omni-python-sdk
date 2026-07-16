from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="FoldersListResponseRecordsItemCount")


@_attrs_define
class FoldersListResponseRecordsItemCount:
    """Count statistics for the folder

    Attributes:
        documents (float): Number of documents in the folder
        favorites (float): Number of users who have favorited this folder
    """

    documents: float
    favorites: float
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        documents = self.documents

        favorites = self.favorites

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "documents": documents,
                "favorites": favorites,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        documents = d.pop("documents")

        favorites = d.pop("favorites")

        folders_list_response_records_item_count = cls(
            documents=documents,
            favorites=favorites,
        )

        folders_list_response_records_item_count.additional_properties = d
        return folders_list_response_records_item_count

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
