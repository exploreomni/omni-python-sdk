from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DbtEnvironmentVariable")


@_attrs_define
class DbtEnvironmentVariable:
    """
    Attributes:
        is_secret (bool): Whether the variable value is secret
        name (str): Variable name
        value (str): Variable value
    """

    is_secret: bool
    name: str
    value: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        is_secret = self.is_secret

        name = self.name

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "isSecret": is_secret,
                "name": name,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        is_secret = d.pop("isSecret")

        name = d.pop("name")

        value = d.pop("value")

        dbt_environment_variable = cls(
            is_secret=is_secret,
            name=name,
            value=value,
        )

        dbt_environment_variable.additional_properties = d
        return dbt_environment_variable

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
