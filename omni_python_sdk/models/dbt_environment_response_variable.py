from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast
from uuid import UUID

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DbtEnvironmentResponseVariable")


@_attrs_define
class DbtEnvironmentResponseVariable:
    """
    Attributes:
        id (UUID): Variable ID
        is_secret (bool): Whether the variable value is secret
        name (str): Variable name
        value (None | str): Variable value (null for secret variables)
    """

    id: UUID
    is_secret: bool
    name: str
    value: None | str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = str(self.id)

        is_secret = self.is_secret

        name = self.name

        value: None | str
        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "isSecret": is_secret,
                "name": name,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = UUID(d.pop("id"))

        is_secret = d.pop("isSecret")

        name = d.pop("name")

        def _parse_value(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        value = _parse_value(d.pop("value"))

        dbt_environment_response_variable = cls(
            id=id,
            is_secret=is_secret,
            name=name,
            value=value,
        )

        dbt_environment_response_variable.additional_properties = d
        return dbt_environment_response_variable

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
