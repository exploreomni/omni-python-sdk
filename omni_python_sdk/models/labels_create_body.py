from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="LabelsCreateBody")


@_attrs_define
class LabelsCreateBody:
    """
    Attributes:
        name (str): Label name Example: important.
        color (None | str | Unset): Hex color for the label (e.g. #0366d6) Example: #0366d6.
        description (None | str | Unset): Label description Example: Important items that need attention.
        homepage (bool | Unset): Show label on homepage. Requires admin permissions. Default: False.
        verified (bool | Unset): Mark as verified label. Requires admin permissions. Default: False.
    """

    name: str
    color: None | str | Unset = UNSET
    description: None | str | Unset = UNSET
    homepage: bool | Unset = False
    verified: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        color: None | str | Unset
        if isinstance(self.color, Unset):
            color = UNSET
        else:
            color = self.color

        description: None | str | Unset
        if isinstance(self.description, Unset):
            description = UNSET
        else:
            description = self.description

        homepage = self.homepage

        verified = self.verified

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if color is not UNSET:
            field_dict["color"] = color
        if description is not UNSET:
            field_dict["description"] = description
        if homepage is not UNSET:
            field_dict["homepage"] = homepage
        if verified is not UNSET:
            field_dict["verified"] = verified

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        def _parse_color(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        color = _parse_color(d.pop("color", UNSET))

        def _parse_description(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        description = _parse_description(d.pop("description", UNSET))

        homepage = d.pop("homepage", UNSET)

        verified = d.pop("verified", UNSET)

        labels_create_body = cls(
            name=name,
            color=color,
            description=description,
            homepage=homepage,
            verified=verified,
        )

        labels_create_body.additional_properties = d
        return labels_create_body

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
