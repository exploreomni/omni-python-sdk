from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="LabelsUpdateBody")


@_attrs_define
class LabelsUpdateBody:
    """
    Attributes:
        color (None | str | Unset): Hex color for the label (e.g. #0366d6) Example: #0366d6.
        description (None | str | Unset): Label description Example: Important items that need attention.
        homepage (bool | Unset): Show label on homepage. Requires admin permissions to modify.
        name (str | Unset): Label name Example: important.
        verified (bool | Unset): Mark as verified label. Requires admin permissions to modify.
    """

    color: None | str | Unset = UNSET
    description: None | str | Unset = UNSET
    homepage: bool | Unset = UNSET
    name: str | Unset = UNSET
    verified: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
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

        name = self.name

        verified = self.verified

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if color is not UNSET:
            field_dict["color"] = color
        if description is not UNSET:
            field_dict["description"] = description
        if homepage is not UNSET:
            field_dict["homepage"] = homepage
        if name is not UNSET:
            field_dict["name"] = name
        if verified is not UNSET:
            field_dict["verified"] = verified

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

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

        name = d.pop("name", UNSET)

        verified = d.pop("verified", UNSET)

        labels_update_body = cls(
            color=color,
            description=description,
            homepage=homepage,
            name=name,
            verified=verified,
        )

        labels_update_body.additional_properties = d
        return labels_update_body

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
