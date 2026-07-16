from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.connection_environments_create_connections_environments_create_response_connection_environment import (
        ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponseConnectionEnvironment,
    )


T = TypeVar("T", bound="ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse")


@_attrs_define
class ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponse:
    """Create connection environments response

    Attributes:
        connection_environments
            (list[ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponseConnectionEnvironment]): Created
            connection environments
    """

    connection_environments: list[
        ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponseConnectionEnvironment
    ]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        connection_environments = []
        for connection_environments_item_data in self.connection_environments:
            connection_environments_item = connection_environments_item_data.to_dict()
            connection_environments.append(connection_environments_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connectionEnvironments": connection_environments,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.connection_environments_create_connections_environments_create_response_connection_environment import (
            ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponseConnectionEnvironment,
        )

        d = dict(src_dict)
        connection_environments = []
        _connection_environments = d.pop("connectionEnvironments")
        for connection_environments_item_data in _connection_environments:
            connection_environments_item = (
                ConnectionEnvironmentsCreateConnectionsEnvironmentsCreateResponseConnectionEnvironment.from_dict(
                    connection_environments_item_data
                )
            )

            connection_environments.append(connection_environments_item)

        connection_environments_create_connections_environments_create_response = cls(
            connection_environments=connection_environments,
        )

        connection_environments_create_connections_environments_create_response.additional_properties = d
        return connection_environments_create_connections_environments_create_response

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
