from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionsDbtGetDbtNotConfiguredResponse")


@_attrs_define
class ConnectionsDbtGetDbtNotConfiguredResponse:
    """Response when dbt is not configured

    Attributes:
        message (str): Message explaining dbt status Example: dbt not configured for this connection.
        supports_dbt (bool): Whether the connection dialect supports dbt Example: True.
    """

    message: str
    supports_dbt: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        supports_dbt = self.supports_dbt

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "message": message,
                "supportsDbt": supports_dbt,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        message = d.pop("message")

        supports_dbt = d.pop("supportsDbt")

        connections_dbt_get_dbt_not_configured_response = cls(
            message=message,
            supports_dbt=supports_dbt,
        )

        connections_dbt_get_dbt_not_configured_response.additional_properties = d
        return connections_dbt_get_dbt_not_configured_response

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
