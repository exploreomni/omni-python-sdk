from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.connections_get_connections_get_response_connection import (
        ConnectionsGetConnectionsGetResponseConnection,
    )


T = TypeVar("T", bound="ConnectionsGetConnectionsGetResponse")


@_attrs_define
class ConnectionsGetConnectionsGetResponse:
    """Get connection response

    Attributes:
        connection (ConnectionsGetConnectionsGetResponseConnection): Connection object
    """

    connection: ConnectionsGetConnectionsGetResponseConnection
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        connection = self.connection.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connection": connection,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.connections_get_connections_get_response_connection import (
            ConnectionsGetConnectionsGetResponseConnection,
        )

        d = dict(src_dict)
        connection = ConnectionsGetConnectionsGetResponseConnection.from_dict(d.pop("connection"))

        connections_get_connections_get_response = cls(
            connection=connection,
        )

        connections_get_connections_get_response.additional_properties = d
        return connections_get_connections_get_response

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
