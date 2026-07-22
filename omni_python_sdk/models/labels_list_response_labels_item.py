from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="LabelsListResponseLabelsItem")


@_attrs_define
class LabelsListResponseLabelsItem:
    """
    Attributes:
        color (None | str): Hex color for the label (e.g. #0366d6) Example: #0366d6.
        description (None | str): Label description Example: Important items that need attention.
        homepage (bool): Whether label is shown on homepage
        name (str): Label name Example: verified.
        usage_count (float): Number of documents with this label
        verified (bool): Whether label is verified
    """

    color: None | str
    description: None | str
    homepage: bool
    name: str
    usage_count: float
    verified: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        color: None | str
        color = self.color

        description: None | str
        description = self.description

        homepage = self.homepage

        name = self.name

        usage_count = self.usage_count

        verified = self.verified

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "color": color,
                "description": description,
                "homepage": homepage,
                "name": name,
                "usage_count": usage_count,
                "verified": verified,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_color(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        color = _parse_color(d.pop("color"))

        def _parse_description(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        description = _parse_description(d.pop("description"))

        homepage = d.pop("homepage")

        name = d.pop("name")

        usage_count = d.pop("usage_count")

        verified = d.pop("verified")

        labels_list_response_labels_item = cls(
            color=color,
            description=description,
            homepage=homepage,
            name=name,
            usage_count=usage_count,
            verified=verified,
        )

        labels_list_response_labels_item.additional_properties = d
        return labels_list_response_labels_item

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
