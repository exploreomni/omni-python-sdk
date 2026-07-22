from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ConnectionsDbtUpdateConnectionsDbtUpdateResponse")


@_attrs_define
class ConnectionsDbtUpdateConnectionsDbtUpdateResponse:
    """dbt update response

    Attributes:
        message (str): Success message Example: dbt configuration updated successfully.
        success (bool): Whether the operation succeeded Example: True.
    """

    message: str
    success: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        success = self.success

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "message": message,
                "success": success,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        message = d.pop("message")

        success = d.pop("success")

        connections_dbt_update_connections_dbt_update_response = cls(
            message=message,
            success=success,
        )

        connections_dbt_update_connections_dbt_update_response.additional_properties = d
        return connections_dbt_update_connections_dbt_update_response

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
