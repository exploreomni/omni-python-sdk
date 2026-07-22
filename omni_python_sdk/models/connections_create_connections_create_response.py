from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionsCreateConnectionsCreateResponse")


@_attrs_define
class ConnectionsCreateConnectionsCreateResponse:
    """Create connection response

    Attributes:
        data (UUID): Created connection ID Example: 550e8400-e29b-41d4-a716-446655440000.
        success (bool): Whether the operation succeeded Example: True.
    """

    data: UUID
    success: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = str(self.data)

        success = self.success

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "data": data,
                "success": success,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        data = UUID(d.pop("data"))

        success = d.pop("success")

        connections_create_connections_create_response = cls(
            data=data,
            success=success,
        )

        connections_create_connections_create_response.additional_properties = d
        return connections_create_connections_create_response

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
