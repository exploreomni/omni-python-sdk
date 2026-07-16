from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0")


@_attrs_define
class ConnectionsUpdateConnectionsUpdateBodyEnvironmentUserAttributeType0:
    """User attribute settings for connection environments

    Attributes:
        attribute_name (str): Name of the user attribute for environment selection Example: region.
        default_values (list[str] | None): Default values for the user attribute Example: ['us-east', 'us-west'].
    """

    attribute_name: str
    default_values: list[str] | None
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        attribute_name = self.attribute_name

        default_values: list[str] | None
        if isinstance(self.default_values, list):
            default_values = self.default_values

        else:
            default_values = self.default_values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "attributeName": attribute_name,
                "defaultValues": default_values,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        attribute_name = d.pop("attributeName")

        def _parse_default_values(data: object) -> list[str] | None:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                default_values_type_0 = cast(list[str], data)

                return default_values_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[str] | None, data)

        default_values = _parse_default_values(d.pop("defaultValues"))

        connections_update_connections_update_body_environment_user_attribute_type_0 = cls(
            attribute_name=attribute_name,
            default_values=default_values,
        )

        connections_update_connections_update_body_environment_user_attribute_type_0.additional_properties = d
        return connections_update_connections_update_body_environment_user_attribute_type_0

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
