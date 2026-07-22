from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_abilities import DocumentAbilities


T = TypeVar("T", bound="DocumentsGetPermissionsResponse")


@_attrs_define
class DocumentsGetPermissionsResponse:
    """
    Attributes:
        abilities (DocumentAbilities): Document-level ability values, as stored on the document
        permits (Any | Unset): User permits for the document. Present only when userId is provided.
    """

    abilities: DocumentAbilities
    permits: Any | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        abilities = self.abilities.to_dict()

        permits = self.permits

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "abilities": abilities,
            }
        )
        if permits is not UNSET:
            field_dict["permits"] = permits

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_abilities import DocumentAbilities

        d = dict(src_dict)
        abilities = DocumentAbilities.from_dict(d.pop("abilities"))

        permits = d.pop("permits", UNSET)

        documents_get_permissions_response = cls(
            abilities=abilities,
            permits=permits,
        )

        documents_get_permissions_response.additional_properties = d
        return documents_get_permissions_response

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
