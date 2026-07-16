from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.role_origin_type_3_type import RoleOriginType3Type, check_role_origin_type_3_type

T = TypeVar("T", bound="RoleOriginType3")


@_attrs_define
class RoleOriginType3:
    """
    Attributes:
        depth (float): Nesting depth of the group
        mini_uuid (str): Short identifier of the group Example: abc123.
        name (str): Name of the group Example: Engineering Team.
        type_ (RoleOriginType3Type): Role inherited from group membership
    """

    depth: float
    mini_uuid: str
    name: str
    type_: RoleOriginType3Type
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        depth = self.depth

        mini_uuid = self.mini_uuid

        name = self.name

        type_: str = self.type_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "depth": depth,
                "miniUuid": mini_uuid,
                "name": name,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        depth = d.pop("depth")

        mini_uuid = d.pop("miniUuid")

        name = d.pop("name")

        type_ = check_role_origin_type_3_type(d.pop("type"))

        role_origin_type_3 = cls(
            depth=depth,
            mini_uuid=mini_uuid,
            name=name,
            type_=type_,
        )

        role_origin_type_3.additional_properties = d
        return role_origin_type_3

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
