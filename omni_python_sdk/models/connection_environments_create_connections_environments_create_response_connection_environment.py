from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponseConnectionEnvironment")


@_attrs_define
class ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponseConnectionEnvironment:
    """Connection environment object

    Attributes:
        base_connection_id (UUID): ID of the base connection Example: 550e8400-e29b-41d4-a716-446655440000.
        connection_id (UUID): ID of the environment connection Example: 550e8400-e29b-41d4-a716-446655440002.
        id (UUID): Unique connection environment identifier Example: 550e8400-e29b-41d4-a716-446655440001.
        user_attribute_values (list[str]): User attribute values for this environment Example: ['us-east',
            'production'].
    """

    base_connection_id: UUID
    connection_id: UUID
    id: UUID
    user_attribute_values: list[str]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base_connection_id = str(self.base_connection_id)

        connection_id = str(self.connection_id)

        id = str(self.id)

        user_attribute_values = self.user_attribute_values

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "baseConnectionId": base_connection_id,
                "connectionId": connection_id,
                "id": id,
                "userAttributeValues": user_attribute_values,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        base_connection_id = UUID(d.pop("baseConnectionId"))

        connection_id = UUID(d.pop("connectionId"))

        id = UUID(d.pop("id"))

        user_attribute_values = cast(list[str], d.pop("userAttributeValues"))

        connection_environments_create_connections_environments_create_response_connection_environment = cls(
            base_connection_id=base_connection_id,
            connection_id=connection_id,
            id=id,
            user_attribute_values=user_attribute_values,
        )

        connection_environments_create_connections_environments_create_response_connection_environment.additional_properties = d
        return connection_environments_create_connections_environments_create_response_connection_environment

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
