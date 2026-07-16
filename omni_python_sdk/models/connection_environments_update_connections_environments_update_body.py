from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionEnvironmentsUpdateConnectionsEnvironmentsUpdateBody")


@_attrs_define
class ConnectionEnvironmentsUpdateConnectionsEnvironmentsUpdateBody:
    """Request body for updating a connection environment

    Attributes:
        user_attribute_values (list[str]): User attribute values for this environment Example: ['us-east',
            'production'].
    """

    user_attribute_values: list[str]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        user_attribute_values = self.user_attribute_values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "userAttributeValues": user_attribute_values,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        user_attribute_values = cast(list[str], d.pop("userAttributeValues"))

        connection_environments_update_connections_environments_update_body = cls(
            user_attribute_values=user_attribute_values,
        )

        connection_environments_update_connections_environments_update_body.additional_properties = d
        return connection_environments_update_connections_environments_update_body

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
