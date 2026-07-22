from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DocumentsUpgradeLayoutResponse")


@_attrs_define
class DocumentsUpgradeLayoutResponse:
    """
    Attributes:
        identifier (str): Document identifier
        upgraded (bool): True when the layout was upgraded, false when the document already had advanced layout (no-op).
    """

    identifier: str
    upgraded: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        identifier = self.identifier

        upgraded = self.upgraded

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "identifier": identifier,
                "upgraded": upgraded,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        identifier = d.pop("identifier")

        upgraded = d.pop("upgraded")

        documents_upgrade_layout_response = cls(
            identifier=identifier,
            upgraded=upgraded,
        )

        documents_upgrade_layout_response.additional_properties = d
        return documents_upgrade_layout_response

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
