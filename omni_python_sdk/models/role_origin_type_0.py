from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.role_origin_type_0_type import RoleOriginType0Type, check_role_origin_type_0_type

T = TypeVar("T", bound="RoleOriginType0")


@_attrs_define
class RoleOriginType0:
    """
    Attributes:
        type_ (RoleOriginType0Type): Role assigned directly to user
    """

    type_: RoleOriginType0Type
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str = self.type_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = check_role_origin_type_0_type(d.pop("type"))

        role_origin_type_0 = cls(
            type_=type_,
        )

        role_origin_type_0.additional_properties = d
        return role_origin_type_0

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
