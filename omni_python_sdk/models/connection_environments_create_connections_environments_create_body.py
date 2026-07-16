from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody")


@_attrs_define
class ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateBody:
    """Request body for creating connection environments

    Attributes:
        base_connection_id (UUID): ID of the base connection Example: 550e8400-e29b-41d4-a716-446655440000.
        environment_connection_ids (list[UUID]): IDs of connections to use as environments Example:
            ['550e8400-e29b-41d4-a716-446655440002', '550e8400-e29b-41d4-a716-446655440003'].
    """

    base_connection_id: UUID
    environment_connection_ids: list[UUID]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        base_connection_id = str(self.base_connection_id)

        environment_connection_ids = []
        for environment_connection_ids_item_data in self.environment_connection_ids:
            environment_connection_ids_item = str(environment_connection_ids_item_data)
            environment_connection_ids.append(environment_connection_ids_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "baseConnectionId": base_connection_id,
                "environmentConnectionIds": environment_connection_ids,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        base_connection_id = UUID(d.pop("baseConnectionId"))

        environment_connection_ids = []
        _environment_connection_ids = d.pop("environmentConnectionIds")
        for environment_connection_ids_item_data in _environment_connection_ids:
            environment_connection_ids_item = UUID(environment_connection_ids_item_data)

            environment_connection_ids.append(environment_connection_ids_item)

        connection_environments_create_connections_environments_create_body = cls(
            base_connection_id=base_connection_id,
            environment_connection_ids=environment_connection_ids,
        )

        connection_environments_create_connections_environments_create_body.additional_properties = d
        return connection_environments_create_connections_environments_create_body

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
